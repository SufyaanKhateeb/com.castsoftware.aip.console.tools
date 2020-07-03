package com.castsoftware.aip.console.tools.commands;

import com.castsoftware.aip.console.tools.core.dto.ApiInfoDto;
import com.castsoftware.aip.console.tools.core.dto.VersionDto;
import com.castsoftware.aip.console.tools.core.dto.VersionStatus;
import com.castsoftware.aip.console.tools.core.dto.jobs.JobRequestBuilder;
import com.castsoftware.aip.console.tools.core.dto.jobs.JobState;
import com.castsoftware.aip.console.tools.core.dto.jobs.JobStatusWithSteps;
import com.castsoftware.aip.console.tools.core.dto.jobs.JobType;
import com.castsoftware.aip.console.tools.core.exceptions.ApiCallException;
import com.castsoftware.aip.console.tools.core.exceptions.ApiKeyMissingException;
import com.castsoftware.aip.console.tools.core.exceptions.ApplicationServiceException;
import com.castsoftware.aip.console.tools.core.exceptions.JobServiceException;
import com.castsoftware.aip.console.tools.core.services.ApplicationService;
import com.castsoftware.aip.console.tools.core.services.JobsService;
import com.castsoftware.aip.console.tools.core.services.RestApiService;
import com.castsoftware.aip.console.tools.core.utils.Constants;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Creates a snapshot for an application
 */
@Component
@CommandLine.Command(
        name = "Snapshot",
        mixinStandardHelpOptions = true,
        aliases = {"snapshot"},
        description = "Runs a snapshot on AIP Console"
)
@Slf4j
@Getter
@Setter
public class SnapshotCommand implements Callable<Integer> {
    private static final DateFormat RELEASE_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private final RestApiService restApiService;
    private final JobsService jobsService;
    private final ApplicationService applicationService;
    @CommandLine.Mixin
    private SharedOptions sharedOptions;

    @CommandLine.Option(names = {"-n", "--app-name"}, paramLabel = "APPLICATION_NAME", description = "The Name of the application to analyze")
    private String applicationName;

    @CommandLine.Option(names = {"-v", "--version-name"}, paramLabel = "VERSION_NAME", description = "The name of the version for which the snapshot will be run")
    private String versionName;

    @CommandLine.Option(names = {"-S", "--snapshot-name"}, paramLabel = "SNAPSHOT_NAME", description = "The name of the snapshot to create")
    private String snapshotName;

    public SnapshotCommand(RestApiService restApiService, JobsService jobsService, ApplicationService applicationService) {
        this.restApiService = restApiService;
        this.jobsService = jobsService;
        this.applicationService = applicationService;
    }


    @Override
    public Integer call() throws Exception {
        // Runs snapshot + upload
        if (StringUtils.isBlank(applicationName)) {
            log.error("No application name provided. Exiting.");
            return Constants.RETURN_APPLICATION_INFO_MISSING;
        }

        try {
            if (sharedOptions.getTimeout() != Constants.DEFAULT_HTTP_TIMEOUT) {
                restApiService.setTimeout(sharedOptions.getTimeout(), TimeUnit.SECONDS);
            }
            restApiService.validateUrlAndKey(sharedOptions.getFullServerRootUrl(), sharedOptions.getUsername(), sharedOptions.getApiKeyValue());
        } catch (ApiKeyMissingException e) {
            return Constants.RETURN_NO_PASSWORD;
        } catch (ApiCallException e) {
            return Constants.RETURN_LOGIN_ERROR;
        }
        ApiInfoDto apiInfoDto = restApiService.getAipConsoleApiInfo();

        try {
            log.info("Searching for application '{}' on AIP Console", applicationName);
            String applicationGuid = applicationService.getApplicationGuidFromName(applicationName);
            if (StringUtils.isBlank(applicationGuid)) {
                log.error("Application '{}' was not found on AIP Console", applicationName);
                return Constants.RETURN_APPLICATION_NOT_FOUND;
            }
            Set<VersionDto> versions = applicationService.getApplicationVersion(applicationGuid);
            if (versions.isEmpty()) {
                log.error("No version for the given application. Cannot run Snapshot without an analyzed version");
                return Constants.RETURN_APPLICATION_NO_VERSION;
            }
            if (versions.stream().noneMatch(v -> v.getStatus().ordinal() >= VersionStatus.ANALYSIS_DONE.ordinal())) {
                log.error("No analysis done for application '{}'. Cannot create snapshot.", applicationName);
                // FIXME: Constant for return code
                return 9;
            }
            String versionGuid;
            if (StringUtils.isNotBlank(versionName)) {
                versionGuid = versions.stream()
                        .filter(v -> StringUtils.equalsIgnoreCase(v.getName(), versionName))
                        .map(VersionDto::getGuid).findFirst().orElse(null);
            } else {
                versionGuid = versions
                        .stream()
                        .filter(v -> v.getStatus().ordinal() >= VersionStatus.ANALYSIS_DONE.ordinal())
                        .max(Comparator.comparing(VersionDto::getVersionDate))
                        .map(VersionDto::getGuid)
                        .orElse(null);
            }
            if (StringUtils.isBlank(versionGuid)) {
                String message = StringUtils.isBlank(versionName) ?
                        "No analyzed version found to create a snapshot for. Make sure you have at least one version that has been analyzed" :
                        "No version found with name " + versionName;
                log.error(message);
                return Constants.RETURN_APPLICATION_VERSION_NOT_FOUND;
            }
            if (StringUtils.isBlank(snapshotName)) {
                snapshotName = RELEASE_DATE_FORMATTER.format(new Date());
            }


            String endStep = Constants.UPLOAD_APP_SNAPSHOT;
            if (apiInfoDto.getApiVersionSemVer().getMajor() <= 1 &&
                    apiInfoDto.getApiVersionSemVer().getMinor() <= 15) {
                endStep = Constants.CONSOLIDATE_SNAPSHOT;
            }

            // Run snapshot
            JobRequestBuilder builder = JobRequestBuilder.newInstance(applicationGuid, null, JobType.ANALYZE)
                    .startStep(Constants.SNAPSHOT_STEP_NAME)
                    .endStep(endStep)
                    .versionGuid(versionGuid)
                    .snapshotName(snapshotName)
                    .releaseAndSnapshotDate(new Date());

            log.info("Running Snapshot Job on application '{}'", applicationName);
            String jobGuid = jobsService.startJob(builder);
            JobStatusWithSteps jobStatus = jobsService.pollAndWaitForJobFinished(jobGuid, Function.identity());
            if (JobState.COMPLETED == jobStatus.getState()) {
                log.info("Snapshot Creation completed successfully.");
                return Constants.RETURN_OK;
            }
            log.error("Snapshot Job did not complete. Status is '{}' on step '{}'", jobStatus.getState(), jobStatus.getFailureStep());
            return Constants.RETURN_JOB_FAILED;
        } catch (ApplicationServiceException e) {
            return Constants.RETURN_APPLICATION_INFO_MISSING;
        } catch (JobServiceException e) {
            return Constants.RETURN_JOB_POLL_ERROR;
        }
    }
}