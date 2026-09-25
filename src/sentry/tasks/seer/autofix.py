import logging

import sentry_sdk
from django.contrib.auth.models import AnonymousUser
from django.utils import timezone
from taskbroker_client.retry import Retry
from taskbroker_client.state import current_task

from sentry import analytics
from sentry.analytics.events.autofix_automation_events import AiAutofixAutomationEvent
from sentry.constants import (
    ObjectStatus,
)
from sentry.models.group import Group
from sentry.models.organization import Organization
from sentry.models.project import Project
from sentry.seer.autofix.constants import (
    AutofixAutomationTuningSettings,
    SeerAutomationSource,
)
from sentry.seer.autofix.issue_summary import (
    get_and_update_group_fixability_score,
    get_issue_summary,
    run_automation,
)
from sentry.seer.autofix.utils import (
    SEAT_BASED_STOPPING_POINTS,
    AutofixStoppingPoint,
    AutomationCodingAgent,
    SeerProjectSettingsUpdate,
    bulk_read_preferences_from_sentry_db,
    get_org_default_seer_automation_handoff,
    get_seer_seat_based_tier_cache_key,
    update_seer_project_settings,
)
from sentry.seer.models.project_repository import SeerProjectRepository
from sentry.tasks.base import instrumented_task
from sentry.taskworker.namespaces import ingest_errors_tasks, issues_tasks
from sentry.utils import metrics
from sentry.utils.cache import cache
from sentry.utils.locking import UnableToAcquireLock

logger = logging.getLogger(__name__)


def _clear_free_autofix_cohort_configuration(organization: Organization) -> None:
    if not organization.get_option("agentic-triage-free-cohort", False):
        return

    SeerProjectRepository.objects.filter(
        project_repository__project__organization_id=organization.id
    ).delete()
    organization.delete_option("agentic-triage-free-cohort")


def _get_group_or_log(group_id: int, task_name: str) -> Group | None:
    """Fetch a Group by ID, returning None and logging a warning if it no longer exists."""
    try:
        return Group.objects.get(id=group_id)
    except Group.DoesNotExist:
        logger.warning("%s.group_not_found", task_name, extra={"group_id": group_id})
        return None


def _generate_issue_summary_and_score(group: Group, source: SeerAutomationSource) -> bool:
    _, status_code = get_issue_summary(group=group, source=source)
    if status_code == 503:
        raise UnableToAcquireLock(
            f"Timed out waiting for issue summary generation for group {group.id}"
        )
    if status_code != 200:
        return False

    get_and_update_group_fixability_score(group)
    return True


@instrumented_task(
    name="sentry.tasks.autofix.generate_issue_summary_only",
    namespace=ingest_errors_tasks,
    processing_deadline_duration=35,
    retry=Retry(times=3, delay=3, on=(Exception,)),
)
def generate_issue_summary_only(
    group_id: int,
    source: SeerAutomationSource | str = SeerAutomationSource.POST_PROCESS,
) -> None:
    """Generate an issue summary and fixability score without running automation."""
    group = _get_group_or_log(group_id, "generate_issue_summary_only")
    if group is None:
        return
    source = SeerAutomationSource(source)
    organization = group.project.organization

    task_state = current_task()
    if task_state is None or task_state.attempt == 0:
        metrics.incr("sentry.tasks.autofix.generate_issue_summary_only", sample_rate=1.0)
        analytics.record(
            AiAutofixAutomationEvent(
                organization_id=organization.id,
                project_id=group.project_id,
                group_id=group.id,
                task_name="generate_issue_summary_only",
                issue_event_count=group.times_seen,
                fixability_score=group.seer_fixability_score,
            )
        )

    _generate_issue_summary_and_score(group, source)


@instrumented_task(
    name="sentry.tasks.autofix.run_issue_automation",
    namespace=ingest_errors_tasks,
    processing_deadline_duration=35,
    retry=Retry(times=5, delay=5, on=(Exception,)),
)
def run_issue_automation(group_id: int, trigger_path: str = "unknown") -> None:
    """Generate the inputs required by automation, then run it."""
    sentry_sdk.set_tag("trigger_path", trigger_path)
    sentry_sdk.set_attribute("trigger_path", trigger_path)

    group = _get_group_or_log(group_id, "run_issue_automation")
    if group is None:
        return
    organization = group.project.organization

    task_state = current_task()
    if task_state is None or task_state.attempt == 0:
        metrics.incr("sentry.tasks.autofix.run_issue_automation", sample_rate=1.0)
        analytics.record(
            AiAutofixAutomationEvent(
                organization_id=organization.id,
                project_id=group.project_id,
                group_id=group.id,
                task_name="run_issue_automation",
                issue_event_count=group.times_seen,
                fixability_score=group.seer_fixability_score,
            )
        )

    if not _generate_issue_summary_and_score(group, SeerAutomationSource.POST_PROCESS):
        return

    event = group.get_latest_event()
    if not event:
        logger.warning("run_issue_automation.no_event_found", extra={"group_id": group_id})
        return

    # Track issue age when running automation
    issue_age_days = int((timezone.now() - group.first_seen).total_seconds() / (60 * 60 * 24))

    metrics.distribution(
        "seer.automation.issue_age_since_first_seen", issue_age_days, unit="day", sample_rate=1.0
    )

    try:
        run_automation(
            group=group,
            user=AnonymousUser(),
            event=event,
            source=SeerAutomationSource.POST_PROCESS,
        )
    except Exception:
        logger.exception(
            "Error auto-triggering autofix from issue summary", extra={"group_id": group.id}
        )


@instrumented_task(
    name="sentry.tasks.autofix.configure_seer_for_existing_org",
    namespace=issues_tasks,
    processing_deadline_duration=90,
    retry=Retry(times=3),
)
def configure_seer_for_existing_org(organization_id: int) -> None:
    """
    Configure Seer settings for a new or existing organization migrating to new Seer pricing.

    Sets:
    - Project-level (all projects): seer_scanner_automation=True, autofix_automation_tuning="medium" or "off"
    - Seer project preferences (all projects): automated_run_stopping_point="code_changes" or "open_pr",
      and automation_handoff backfilled from the org defaults (when set) for projects that don't already
      have one configured.

    Ignores:
    - Org-level: enable_seer_coding
    """

    try:
        organization = Organization.objects.get(id=organization_id)
    except Organization.DoesNotExist:
        logger.warning(
            "configure_seer_for_existing_org.organization_not_found",
            extra={"organization_id": organization_id},
        )
        return

    sentry_sdk.set_tag("organization_id", organization.id)
    sentry_sdk.set_attribute("organization_id", organization.id)
    sentry_sdk.set_tag("organization_slug", organization.slug)
    sentry_sdk.set_attribute("organization_slug", organization.slug)
    _clear_free_autofix_cohort_configuration(organization)

    # Set org-level options
    organization.update_option(
        "sentry:default_autofix_automation_tuning", AutofixAutomationTuningSettings.MEDIUM
    )

    projects = list(
        Project.objects.filter(organization_id=organization_id, status=ObjectStatus.ACTIVE)
    )
    project_ids = [p.id for p in projects]

    if len(project_ids) == 0:
        return

    # If seer is enabled for an org, every project must have project level settings
    for project in projects:
        project.update_option("sentry:seer_scanner_automation", True)
        autofix_automation_tuning = project.get_option("sentry:autofix_automation_tuning")
        if autofix_automation_tuning != AutofixAutomationTuningSettings.OFF:
            project.update_option(
                "sentry:autofix_automation_tuning", AutofixAutomationTuningSettings.MEDIUM
            )

    default_stopping_point, default_handoff = get_org_default_seer_automation_handoff(organization)
    preferences = bulk_read_preferences_from_sentry_db(organization_id, project_ids)

    # Determine which projects need updates
    preferences_set = 0
    for project in projects:
        stopping_point = default_stopping_point
        handoff = default_handoff

        existing_pref = preferences.get(project.id)
        if existing_pref:
            existing_stopping_point = existing_pref.automated_run_stopping_point
            existing_handoff = existing_pref.automation_handoff

            # Skip projects that a) already have an acceptable stopping point configured
            # AND b) already have a handoff configured or no org default handoff.
            if existing_stopping_point in SEAT_BASED_STOPPING_POINTS and (
                existing_handoff or default_handoff is None
            ):
                continue

            if existing_stopping_point in SEAT_BASED_STOPPING_POINTS:
                stopping_point = existing_stopping_point
            if existing_handoff:
                handoff = existing_handoff

        update = SeerProjectSettingsUpdate(stopping_point=stopping_point)
        if handoff is not None:
            update["agent"] = AutomationCodingAgent(handoff.target)
            update["integration_id"] = handoff.integration_id
            update["auto_create_pr"] = handoff.auto_create_pr
        else:
            update["agent"] = AutomationCodingAgent.SEER
            update["auto_create_pr"] = stopping_point == AutofixStoppingPoint.OPEN_PR

        update_seer_project_settings([project.id], update)
        preferences_set += 1

    # Invalidate existing cache entry and set cache to True to prevent race conditions where another
    # request re-caches False before the billing flag has fully propagated
    cache.set(get_seer_seat_based_tier_cache_key(organization_id), True, timeout=60 * 5)

    logger.info(
        "Task: configure_seer_for_existing_org completed",
        extra={
            "org_id": organization.id,
            "org_slug": organization.slug,
            "projects_configured": len(project_ids),
            "preferences_set": preferences_set,
        },
    )
