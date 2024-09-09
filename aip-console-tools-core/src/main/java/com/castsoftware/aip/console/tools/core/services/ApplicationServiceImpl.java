package com.castsoftware.aip.console.tools.core.services;

import com.castsoftware.aip.console.tools.core.dto.ApiInfoDto;
import com.castsoftware.aip.console.tools.core.dto.ApplicationCommonDetailsDto;
import com.castsoftware.aip.console.tools.core.dto.ApplicationDto;
import com.castsoftware.aip.console.tools.core.dto.ApplicationOnboardingDto;
import com.castsoftware.aip.console.tools.core.dto.Applications;
import com.castsoftware.aip.console.tools.core.dto.BaseDto;
import com.castsoftware.aip.console.tools.core.dto.tcc.CheckRuleContentRequest;
import com.castsoftware.aip.console.tools.core.dto.tcc.ComputeFunctionPointsProperties;
import com.castsoftware.aip.console.tools.core.dto.DebugOptionsDto;
import com.castsoftware.aip.console.tools.core.dto.DeepAnalyzeProperties;
import com.castsoftware.aip.console.tools.core.dto.DeliveryConfigurationDto;
import com.castsoftware.aip.console.tools.core.dto.DomainDto;
import com.castsoftware.aip.console.tools.core.dto.ExclusionRuleDto;
import com.castsoftware.aip.console.tools.core.dto.Exclusions;
import com.castsoftware.aip.console.tools.core.dto.FastScanProperties;
import com.castsoftware.aip.console.tools.core.dto.ImagingSettingsDto;
import com.castsoftware.aip.console.tools.core.dto.JsonDto;
import com.castsoftware.aip.console.tools.core.dto.ModuleGenerationType;
import com.castsoftware.aip.console.tools.core.dto.PendingResultDto;
import com.castsoftware.aip.console.tools.core.dto.VersionDto;
import com.castsoftware.aip.console.tools.core.dto.VersionStatus;
import com.castsoftware.aip.console.tools.core.dto.jobs.DeliveryPackageDto;
import com.castsoftware.aip.console.tools.core.dto.jobs.DiscoverPackageRequest;
import com.castsoftware.aip.console.tools.core.dto.jobs.FileCommandRequest;
import com.castsoftware.aip.console.tools.core.dto.jobs.JobRequestBuilder;
import com.castsoftware.aip.console.tools.core.dto.jobs.JobState;
import com.castsoftware.aip.console.tools.core.dto.jobs.LogPollingProvider;
import com.castsoftware.aip.console.tools.core.dto.jobs.ScanAndReScanApplicationJobRequest;
import com.castsoftware.aip.console.tools.core.dto.tcc.FunctionPointRuleDto;
import com.castsoftware.aip.console.tools.core.dto.tcc.RuleContentDto;
import com.castsoftware.aip.console.tools.core.exceptions.ApiCallException;
import com.castsoftware.aip.console.tools.core.exceptions.ApplicationServiceException;
import com.castsoftware.aip.console.tools.core.exceptions.JobServiceException;
import com.castsoftware.aip.console.tools.core.exceptions.PackagePathInvalidException;
import com.castsoftware.aip.console.tools.core.exceptions.UploadException;
import com.castsoftware.aip.console.tools.core.utils.ApiEndpointHelper;
import com.castsoftware.aip.console.tools.core.utils.Constants;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class ApplicationServiceImpl implements ApplicationService {

    private RestApiService restApiService;
    private JobsService jobService;
    private UploadService uploadService;

    public ApplicationServiceImpl(RestApiService restApiService, JobsService jobsService, UploadService uploadService) {
        this.restApiService = restApiService;
        jobService = jobsService;
        this.uploadService = uploadService;
    }

    @Override
    public String getApplicationGuidFromName(String applicationName) throws ApplicationServiceException {
        ApplicationDto app = getApplicationFromName(applicationName);
        return app == null ? null : app.getGuid();
    }

    @Override
    public ApiInfoDto getAipConsoleApiInfo() {
        return restApiService.getAipConsoleApiInfo();
    }

    @Override
    public int fastScan(FastScanProperties fastScanProperties) throws JobServiceException, PackagePathInvalidException, UploadException {
        String applicationName = fastScanProperties.getApplicationName();
        log.info("Fast-Scan args:");
        log.info(String.format("\tApplication: %s%n\tFile: %s%n\tverbose: %s%n\tsleep: %d%n"
                , applicationName, fastScanProperties.getFilePath().getAbsolutePath()
                , fastScanProperties.isVerbose(), fastScanProperties.getSleepDuration()));

        String applicationGuid;
        try {

            log.info("Searching for application '{}' on CAST Imaging Console", applicationName);
            ApplicationCommonDetailsDto applicationCommonDetailsDto = getApplicationDetailsFromName(applicationName);
            if(applicationCommonDetailsDto == null){
                log.info("Application not found, starting new upload");
            }
            String sourcePath = uploadService.uploadFileForOnboarding(
                    fastScanProperties.getFilePath()
                    , applicationCommonDetailsDto != null ? applicationCommonDetailsDto.getGuid() : null);
            log.info("Uploaded sources successfully, source path: " + sourcePath);

            if (applicationCommonDetailsDto == null) {
                applicationGuid = onboardApplication(applicationName
                        , fastScanProperties.getDomainName(), fastScanProperties.isVerbose(), sourcePath);
                log.info("Onboard Application job has started, application ID: " + applicationGuid);
            } else {
                applicationGuid = applicationCommonDetailsDto.getGuid();
            }

            ApplicationOnboardingDto applicationOnboardingDto = getApplicationOnboarding(applicationGuid);
            String caipVersion = applicationOnboardingDto.getCaipVersion();
            String targetNode = applicationOnboardingDto.getTargetNode();

            DeliveryConfigurationDto deliveryConfiguration = null;
            Exclusions exclusions = Exclusions.builder().excludePatterns(fastScanProperties.getExclusionPatterns()).build();
            if (fastScanProperties.getExclusionRules() != null && fastScanProperties.getExclusionRules().length > 0) {
                exclusions.setInitialExclusionRules(fastScanProperties.getExclusionRules());
            }

            //discover-packages
            log.info("Preparing the Application Delivery Configuration");
            DeliveryConfigurationDto[] deliveryConfig = new DeliveryConfigurationDto[1];
            String deliveryConfigurationGuid = discoverPackagesAndCreateDeliveryConfiguration(applicationGuid, sourcePath, exclusions,
                    VersionStatus.DELIVERED, true, (config) -> deliveryConfig[0] = config, true);
            deliveryConfiguration = deliveryConfig[0];
            log.info("Application Delivery Configuration done: GUID=" + deliveryConfigurationGuid);
            deliveryConfiguration.setGuid(deliveryConfigurationGuid);

            log.info("Starting Fast-Scan action with Delivery Configuration Guid=" + deliveryConfiguration.getGuid());
            String jobStatus = fastScan(applicationGuid, sourcePath, "", deliveryConfiguration,
                    caipVersion, targetNode, fastScanProperties.isVerbose(), fastScanProperties.getLogPollingProvider());
            if (jobStatus != null && jobStatus.equalsIgnoreCase(JobState.COMPLETED.toString())) {
                log.info("Fast-Scan done successfully");
            } else {
                log.info("Fast-Scan not completed successfully");
                return Constants.RETURN_JOB_FAILED;
            }
        } catch (ApplicationServiceException e) {
            return Constants.RETURN_APPLICATION_INFO_MISSING;
        }

        return Constants.RETURN_OK;
    }

    @Override
    public int deepAnalyze(DeepAnalyzeProperties properties) throws JobServiceException {
        log.info("Deep-Analyze command will perform application '{}' deep analysis: verbose= '{}'"
                , properties.getApplicationName(), properties.isVerbose());
        log.info("Deep-Analysis args:");
        log.info("\nApplication: {}\nsnapshot name: {}\nmodule generation type: {}\nprocess imaging: {}\nsleep: {}\n"
                , properties.getApplicationName(), StringUtils.isEmpty(properties.getSnapshotName()) ? "Auto assigned" : properties.getSnapshotName()
                , properties.getModuleGenerationType().toString(), properties.isProcessImaging(), properties.getSleepDuration());

        try {
            boolean isProcessImaging = properties.isProcessImaging();
            boolean isPublishToEngineering = properties.isPublishToEngineering();

            String applicationName = properties.getApplicationName();
            log.info("Searching for application '{}' on CAST Imaging Console", applicationName);
            String existingAppGuid = null;
            ApplicationCommonDetailsDto applicationCommonDetailsDto = getApplicationDetailsFromName(applicationName);
            if (applicationCommonDetailsDto != null) {
                existingAppGuid = applicationCommonDetailsDto.getGuid();
            } else {
                log.error("Unable to trigger Deep-Analysis. The actual conditions required Fast-Scan to be running first.");
                return Constants.RETURN_ONBOARD_FAST_SCAN_REQUIRED;
            }

            if (isPublishToEngineering && StringUtils.isBlank(properties.getSnapshotName())) {
                String snapshotName=String.format("Snapshot-%s", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new Date()));
                properties.setSnapshotName(snapshotName);
                log.info("A default snapshot name has been given: {}",snapshotName);
            }

            log.info("About to trigger deep-analysis for application: {}", applicationName);
            if (StringUtils.isNotEmpty(properties.getSnapshotName())) {
                log.info("  With snapshot name: " + properties.getSnapshotName());
            }
            ApplicationOnboardingDto applicationOnboardingDto = getApplicationOnboarding(existingAppGuid);
            String caipVersion = applicationOnboardingDto.getCaipVersion();
            String targetNode = applicationOnboardingDto.getTargetNode();

            //Run Analysis
            String jobStatus = runDeepAnalysis(existingAppGuid, targetNode, caipVersion
                    , isProcessImaging, properties.isPublishToEngineering(), properties.getSnapshotName()
                    , properties.getModuleGenerationType(), properties.isVerbose(), properties.getLogPollingProvider());
            if (jobStatus != null && jobStatus.equalsIgnoreCase(JobState.COMPLETED.toString())) {
                log.info("Deep-Analyze done successfully");
            } else {
                log.error("Deep-Analyze didn't completed successfully.");
                return Constants.RETURN_JOB_FAILED;
            }
        } catch (ApplicationServiceException e) {
            return Constants.RETURN_APPLICATION_INFO_MISSING;
        }

        return Constants.RETURN_OK;
    }

    @Override
    public int publishToImaging(String applicationName, long sleepDuration, boolean verbose, LogPollingProvider logPollingProvider) {
        log.info("Publishing application '{}' data to CAST Imaging with verbose= '{}'", applicationName, verbose);
        try {
            log.info("Searching for application '{}' on CAST Imaging Console", applicationName);
            ApplicationDto applicationDto = getApplicationFromName(applicationName);
            if (applicationDto == null) {
                log.error("No action to perform: application '{}' does not exist.", applicationName);
                return Constants.RETURN_APPLICATION_NOT_FOUND;
            }

            if (!isOnboardingSettingsEnabled()) {
                log.info("The 'Onboard Application' mode is OFF on CAST Imaging Console: Set it ON before proceed");
                //applicationService.setEnableOnboarding(true);
                return Constants.RETURN_ONBOARD_APPLICATION_DISABLED;
            }
            Set<VersionDto> versions = getApplicationVersion(applicationDto.getGuid());
            if (versions == null || versions.isEmpty()) {
                log.error("No version for the given application. Make sure at least one version has been delivered");
                return Constants.RETURN_APPLICATION_NO_VERSION;
            }

            applicationDto = getApplicationDetails(applicationDto.getGuid());
            Set<String> statuses = EnumSet.of(VersionStatus.ANALYSIS_DATA_PREPARED, VersionStatus.IMAGING_PROCESSED,
                    VersionStatus.SNAPSHOT_DONE, VersionStatus.FULLY_ANALYZED, VersionStatus.ANALYZED)
                    .stream().map(VersionStatus::toString).collect(Collectors.toSet());
            VersionDto versionDto = applicationDto.getVersion();
            if (!statuses.contains(versionDto.getStatus().toString())) {
                log.error("Application version not in the status that allows application data to be published to CAST Imaging: actual status is " + versionDto.getStatus().toString());
                return Constants.RETURN_ONBOARD_VERSION_STATUS_INVALID;
            }

            if (applicationDto.isOnboarded()) {
                log.info("Triggering Publish to Imaging for an application using Fast-Scan workflow.");
                ScanAndReScanApplicationJobRequest.ScanAndReScanApplicationJobRequestBuilder requestBuilder = ScanAndReScanApplicationJobRequest.builder()
                        .appGuid(applicationDto.getGuid());
                String targetNode = applicationDto.getTargetNode();
                if (StringUtils.isNotEmpty(targetNode)) {
                    requestBuilder.targetNode(targetNode);
                }
                String caipVersion = applicationDto.getCaipVersion();
                if (StringUtils.isNotEmpty(caipVersion)) {
                    requestBuilder.caipVersion(caipVersion);
                }
                requestBuilder.processImaging(true);

                String jobStatus = runDeepAnalysis(requestBuilder.build(), logPollingProvider);
                if (jobStatus != null && jobStatus.equalsIgnoreCase(JobState.COMPLETED.toString())) {
                    log.info("Publishing to Imaging done successfully");
                } else {
                    log.error("Publishing to Imaging not completed successfully");
                    return Constants.RETURN_JOB_FAILED;
                }
                return Constants.RETURN_OK;
            }

            String jobStatus = publishToImaging(applicationDto.getGuid(), logPollingProvider);
            if (jobStatus != null && jobStatus.equalsIgnoreCase(JobState.COMPLETED.toString())) {
                log.info("Publish to CAST Imaging done successfully");
            } else {
                log.error("\"Publish to CAST Imaging didn't completed successfully.");
                return Constants.RETURN_JOB_FAILED;
            }
        } catch (ApplicationServiceException e) {
            return Constants.RETURN_APPLICATION_INFO_MISSING;
        }
        return Constants.RETURN_OK;

    }

    @Override
    public int computeFunctionPoints(ComputeFunctionPointsProperties computeFunctionPointsProperties) throws JobServiceException {
        String applicationName = computeFunctionPointsProperties.getApplicationName();
        log.info("Args:");
        log.info(String.format("\tApplication: %s%n\tverbose: %s%n\twait: %s%n",
                applicationName, computeFunctionPointsProperties.isVerbose(), computeFunctionPointsProperties.isWait()));

        String applicationGuid;
        try {
            log.info("Searching for application '{}' on CAST Imaging Console.", applicationName);
            ApplicationCommonDetailsDto applicationCommonDetailsDto = getApplicationDetailsFromName(applicationName);
            if (applicationCommonDetailsDto == null) {
                log.info("Application not found.");
                return Constants.RETURN_APPLICATION_NOT_FOUND;
            }
            applicationGuid = applicationCommonDetailsDto.getGuid();
            ApplicationDto applicationDto = getApplicationDetails(applicationGuid);

            if(!applicationDto.isManaged()) {
                log.info("Compute function points is not available for this application. This maybe because the application has not been analyzed or the analysis is in process.");
                return Constants.RETURN_APPLICATION_INFO_MISSING;
            }

            String targetNode = applicationDto.getTargetNode();
            LogPollingProvider logPollingProvider = computeFunctionPointsProperties.getLogPollingProvider();

            log.info("Starting Compute function points action for application '{}'.", applicationName);
            log.info("Starting Compute function points job for application GUID = '{}'.", applicationGuid);

            String jobGuid = jobService.startComputeFunctionPoints(applicationGuid, targetNode);

            log.info("Compute function points job is ongoing: GUID= '{}'.", jobGuid);

            if(!computeFunctionPointsProperties.isWait()) {
                log.info("Exiting as wait flag is set to false.");
                return Constants.RETURN_OK;
            }

            String jobStatus = logPollingProvider != null ? logPollingProvider.pollJobLog(jobGuid) : null;
            if (jobStatus != null && jobStatus.equalsIgnoreCase(JobState.COMPLETED.toString())) {
                log.info("Compute function points completed successfully.");
            } else {
                log.info("Compute function points failed.");
                return Constants.RETURN_JOB_FAILED;
            }
        } catch (ApplicationServiceException e) {
            return Constants.RETURN_APPLICATION_INFO_MISSING;
        } catch (JobServiceException e) {
            return Constants.RETURN_JOB_FAILED;
        }

        return Constants.RETURN_OK;
    }

    @Override
    public int listFunctionPointRules(String applicationName, String ruleType) {
        String applicationGuid;
        try {
            ApplicationCommonDetailsDto applicationCommonDetailsDto = getApplicationDetailsFromName(applicationName);
            if (applicationCommonDetailsDto == null) {
                log.info("Application not found.");
                return Constants.RETURN_APPLICATION_NOT_FOUND;
            }
            applicationGuid = applicationCommonDetailsDto.getGuid();
            ApplicationDto applicationDto = getApplicationDetails(applicationGuid);

            if (!applicationDto.isManaged()) {
                log.info("List rules is not available for this application. This maybe because the application has not been analyzed or the analysis is in process.");
                return Constants.RETURN_APPLICATION_INFO_MISSING;
            }

            List<FunctionPointRuleDto> functionPointRules = restApiService.getForEntity(ApiEndpointHelper.getApplicationFunctionPointRules(applicationGuid), new TypeReference<List<FunctionPointRuleDto>>() {});
            Map<String, List<FunctionPointRuleDto>> groupedByType = functionPointRules.stream().collect(Collectors.groupingBy(rule -> rule.getType().toLowerCase()));

            if(ruleType != null && !groupedByType.containsKey(ruleType.toLowerCase())) {
                log.error("Invalid rule type");
                return Constants.RETURN_INVALID_PARAMETERS_ERROR;
            }

            if(groupedByType.size() > 0) {
                log.info("=========================================\n");
            }

            groupedByType.forEach((type, list) -> {
                if(ruleType != null && !ruleType.toLowerCase().equals(type)) return;

                log.info("Type: " + type);
                for (int i = 0; i < list.size(); i++) {
                    if(i == list.size() - 1) {
                        log.info(list.get(i).toString());
                    } else {
                        log.info(list.get(i).toString() + ',');
                    }
                }
                log.info("=========================================\n");
            });

        } catch(Exception e) {
            log.error("Something went wrong.", e);
        }
        return Constants.RETURN_OK;
    }

    @Override
    public int checkRuleContent(String applicationName, String ruleId, String ruleType) {
        String applicationGuid;
        try {
            log.info("Searching for application '{}' on CAST Imaging Console.", applicationName);
            ApplicationCommonDetailsDto applicationCommonDetailsDto = getApplicationDetailsFromName(applicationName);
            if (applicationCommonDetailsDto == null) {
                log.info("Application not found.");
                return Constants.RETURN_APPLICATION_NOT_FOUND;
            }
            applicationGuid = applicationCommonDetailsDto.getGuid();
            ApplicationDto applicationDto = getApplicationDetails(applicationGuid);

            if (!applicationDto.isManaged()) {
                log.info("Rules are not available for this application. This maybe because the application has not been analyzed or the analysis is in process.");
                return Constants.RETURN_APPLICATION_INFO_MISSING;
            }
            String filterFactor = ruleId != null ? "id" : "type";
            String filterFactorValue = ruleId != null ? ruleId : ruleType;
            log.info("Finding content for rule with {} '{}'.", filterFactor, filterFactorValue);
            CheckRuleContentRequest.CheckRuleContentRequestBuilder checkRuleContentRequestBuilder = CheckRuleContentRequest.builder();
            if(ruleId != null) {
                checkRuleContentRequestBuilder.ruleId(ruleId);
            }
            if(ruleType != null) {
                checkRuleContentRequestBuilder.ruleType(ruleType);
            }
            List<RuleContentDto> ruleContents = restApiService.postForEntity(ApiEndpointHelper.getApplicationRuleContent(applicationGuid), checkRuleContentRequestBuilder.build(), new TypeReference<List<RuleContentDto>>() {});
            if(ruleContents.isEmpty()) {
                log.info("No content available for rule with {} '{}'.", filterFactor, filterFactorValue);
            } else {
                log.info("Printing the content for rule with {} '{}'.", filterFactor, filterFactorValue);
                for (int i = 0; i < ruleContents.size(); i++) {
                    if(i == ruleContents.size() - 1) {
                        log.info(ruleContents.get(i).toString());
                    } else {
                        log.info(ruleContents.get(i).toString() + ',');
                    }
                }
            }
        } catch(Exception e) {
            log.error("Something went wrong.", e);
        }
        return Constants.RETURN_OK;
    }

    @Override
    public int updateFunctionPointSettings(String applicationName, Map<String, String> settingValueMap) {
        String applicationGuid;
        try {
            ApplicationCommonDetailsDto applicationCommonDetailsDto = getApplicationDetailsFromName(applicationName);
            if (applicationCommonDetailsDto == null) {
                log.info("Application not found.");
                return Constants.RETURN_APPLICATION_NOT_FOUND;
            }
            applicationGuid = applicationCommonDetailsDto.getGuid();
            ApplicationDto applicationDto = getApplicationDetails(applicationGuid);

            if (!applicationDto.isManaged()) {
                log.info("Action not available for this application. This maybe because the application has not been analyzed or the analysis is in process.");
                return Constants.RETURN_APPLICATION_INFO_MISSING;
            }
            for(Map.Entry<String, String> setting: settingValueMap.entrySet()) {
                restApiService.putForEntity(ApiEndpointHelper.getUpdateFunctionPointSettingEndPoint(applicationGuid, setting.getKey(), setting.getValue()), null, String.class);
                log.info("Updated {} to {}", setting.getKey(), setting.getValue());
            }
            log.info("Updated settings successfully.");
        } catch (ApplicationServiceException | ApiCallException e) {
            throw new RuntimeException(e);
        }
        return Constants.RETURN_OK;
    }

    @Override
    public boolean checkServerFoldersExists(String pathToCheck) {
        try {
            FileCommandRequest fileCommandRequest = FileCommandRequest.builder().command("LS").path("SOURCES:" + Paths.get(pathToCheck)).build();
            restApiService.postForEntity("/api/server-folders", fileCommandRequest, String.class);
        } catch (ApiCallException e) {
            return false;
        }
        return true;
    }

    @Override
    public String downloadDeliveryReport(String appGuid, String versionGuid, String reportFilename) throws ApplicationServiceException {
        try {
            return restApiService.getForEntity("/api/applications/" + appGuid + "/versions/" + versionGuid + "/dmt-report/download", String.class);
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Failed to download the delivery report", e);
        }
    }

    @Override
    public String getApplicationNameFromGuid(String applicationGuid) throws ApplicationServiceException {
        ApplicationDto app = getApplicationFromGuid(applicationGuid);
        return app == null ? null : app.getName();
    }

    @Override
    public ApplicationDto getApplicationFromGuid(String applicationGuid) throws ApplicationServiceException {
        return getApplications()
                .getApplications()
                .stream()
                .filter(Objects::nonNull)
                .filter(a -> StringUtils.equalsAnyIgnoreCase(applicationGuid, a.getGuid()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ApplicationDto getApplicationDetails(String applicationGuid) throws ApplicationServiceException {
        try {
            return restApiService.getForEntity(ApiEndpointHelper.getApplicationPath(applicationGuid), ApplicationDto.class);
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get an application with GUID: " + applicationGuid, e);
        }
    }

    @Override
    public ApplicationDto getApplicationFromName(String applicationName) throws ApplicationServiceException {
        return getApplications()
                .getApplications()
                .stream()
                .filter(Objects::nonNull)
                .filter(a -> StringUtils.equalsAnyIgnoreCase(applicationName, a.getName()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ApplicationCommonDetailsDto getApplicationDetailsFromName(String applicationName) throws ApplicationServiceException {
        List<ApplicationCommonDetailsDto> applicationCommonDetails = getApplicationCommonDetails();
        return applicationCommonDetails.stream()
                .filter(Objects:: nonNull)
                .filter(app -> StringUtils.equalsAnyIgnoreCase(applicationName, app.getName()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public DomainDto getDomainFromName(String domainName) throws ApplicationServiceException {
        return getDomains()
                .stream()
                .filter(Objects::nonNull)
                .filter(a -> StringUtils.equalsAnyIgnoreCase(domainName, a.getName()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public boolean applicationHasVersion(String applicationGuid) throws ApplicationServiceException {
        Set<VersionDto> appVersions = getApplicationVersion(applicationGuid);
        return appVersions != null &&
                !appVersions.isEmpty();
    }

    @Override
    public Date getVersionDate(String versionDateString) throws ApplicationServiceException {
        if (StringUtils.isEmpty(versionDateString)) {
            return new Date();
        } else {
            try {
                return JobRequestBuilder.RELEASE_DATE_FORMATTER.parse(versionDateString + ".000Z");
            } catch (ParseException e) {
                log.error("Version release date doesn't match the expected date format");
                throw new ApplicationServiceException("Version release date doesn't match the expected date format", e);
            }
        }
    }

    @Override
    public String getOrCreateApplicationFromName(String applicationName, boolean autoCreate) throws ApplicationServiceException {
        return getOrCreateApplicationFromName(applicationName, autoCreate, null);
    }

    @Override
    public String getOrCreateApplicationFromName(String applicationName, boolean autoCreate, String nodeName) throws ApplicationServiceException {
        return getOrCreateApplicationFromName(applicationName, autoCreate, nodeName, null, null, true);
    }

    @Override
    public String getOrCreateApplicationFromName(String applicationName, boolean autoCreate, String nodeName, String domainName, String cssServerName, boolean verbose) throws ApplicationServiceException {
        if (StringUtils.isBlank(applicationName)) {
            throw new ApplicationServiceException("No application name provided.");
        }

        Optional<ApplicationDto> appDto = getApplications()
                .getApplications()
                .stream()
                .filter(Objects::nonNull)
                .filter(a -> StringUtils.equalsAnyIgnoreCase(applicationName, a.getName()))
                .findFirst();

        if (!appDto.isPresent()) {
            if (!autoCreate) {
                return null;
            }
            try {
                String infoMessage = String.format("Application '%s' not found and 'auto create' enabled. Starting application creation", applicationName);
                if (nodeName != null) {
                    infoMessage += " on node " + nodeName;
                }
                log.info(infoMessage);

                String cssServerGuid = jobService.getCssGuid(cssServerName);
                if(cssServerGuid != null) {
                    log.info("Application " + applicationName + " data repository will stored in CSS Server " + cssServerName + "(guid: " + cssServerGuid + ")");
                } else {
                    log.info("Application " + applicationName + " data repository will stored on default CSS server");
                }

                String jobGuid = jobService.startCreateApplication(applicationName, nodeName, domainName, false, null, cssServerName);
                return jobService.pollAndWaitForJobFinished(jobGuid, (s) -> s.getState() == JobState.COMPLETED ? s.getJobParameters().get("appGuid") : null, verbose);
            } catch (JobServiceException e) {
                log.error("Could not create the application due to the following error", e);
                throw new ApplicationServiceException("Unable to create application automatically.", e);
            }
        }
        return appDto.get().getGuid();
    }

    @Override
    public String onboardApplication(String applicationName, String domainName, boolean verbose, String sourcePath) throws ApplicationServiceException {
        if (StringUtils.isBlank(applicationName)) {
            throw new ApplicationServiceException("No application name provided.");
        }
        log.info("Starting job to onboard Application: " + applicationName);
        try {
            return jobService.startOnboardApplication(applicationName, null, domainName, null);
        } catch (JobServiceException e) {
            log.error("Could not create the application due to the following error", e);
            throw new ApplicationServiceException("Unable to create application automatically.", e);
        }
    }

    @Override
    public String fastScan(String applicationGuid, String sourcePath, String versionName, DeliveryConfigurationDto deliveryConfig, String caipVersion,
                           String targetNode, boolean verbose, LogPollingProvider logPollingProvider) throws ApplicationServiceException {
        try {
            String discoverAction = StringUtils.isNotEmpty(applicationGuid) ? "Refresh" : "Onboard";
            log.info("Starting Fast-Scan job" + (StringUtils.isNotEmpty(applicationGuid) ? " for application GUID= " + applicationGuid : ""));
            String jobGuid = jobService.startFastScan(applicationGuid, sourcePath, versionName, deliveryConfig, caipVersion, targetNode);
            log.info(discoverAction + " Fast-Scan job is ongoing: GUID= " + jobGuid);
            return logPollingProvider != null ? logPollingProvider.pollJobLog(jobGuid) : null;
        } catch (JobServiceException e) {
            log.error("Could not perform Fast-Scan action due to following error", e);
            throw new ApplicationServiceException("Unable to perform Fast-Scan action.", e);
        }
    }

    @Override
    public String discoverApplication(String applicationGuid, String sourcePath, String versionName, String caipVersion,
                                      String targetNode, boolean verbose, LogPollingProvider logPollingProvider) throws ApplicationServiceException {
        try {
            String discoverAction = StringUtils.isNotEmpty(applicationGuid) ? "Refresh" : "Onboard";
            log.info("Starting Discover Application job" + (StringUtils.isNotEmpty(applicationGuid) ? " for application GUID= " + applicationGuid : ""));
            String jobGuid = jobService.startDiscoverApplication(applicationGuid, sourcePath, versionName, caipVersion, targetNode);
            log.info(discoverAction + " Application running job GUID= " + jobGuid);
            return logPollingProvider != null ? logPollingProvider.pollJobLog(jobGuid) : null;
        } catch (JobServiceException e) {
            log.error("Could not discover application contents due to following error", e);
            throw new ApplicationServiceException("Unable to discover application contents automatically.", e);
        }
    }
    
    @Override
    public String runDeepAnalysis(String applicationGuid, String targetNode, String caipVersion
            , boolean isProcessImaging, boolean publishToEngineering, String snapshotName
            , ModuleGenerationType moduleGenerationType, boolean verbose, LogPollingProvider logPollingProvider) throws ApplicationServiceException {
        ScanAndReScanApplicationJobRequest.ScanAndReScanApplicationJobRequestBuilder requestBuilder = ScanAndReScanApplicationJobRequest.builder()
                .appGuid(applicationGuid);
        if (StringUtils.isNotEmpty(targetNode)) {
            requestBuilder.targetNode(targetNode);
        }
        if (StringUtils.isNotEmpty(caipVersion)) {
            requestBuilder.caipVersion(caipVersion);
        }
        if (StringUtils.isNotEmpty(snapshotName)) {
            requestBuilder.snapshotName(snapshotName);
        }
        //The module parameter should be left empty or null when dealing with full content
        if (moduleGenerationType != null
                && (moduleGenerationType != ModuleGenerationType.PRESERVE_CONFIGURED)
                && (moduleGenerationType != ModuleGenerationType.FULL_CONTENT)) {
            requestBuilder.moduleGenerationType(moduleGenerationType.toString());
        }
        requestBuilder.processImaging(isProcessImaging);
        requestBuilder.publishToEngineering(publishToEngineering);
        requestBuilder.uploadApplication(publishToEngineering);
        return runDeepAnalysis(requestBuilder.build(), logPollingProvider);
    }

    @Override
    public String runDeepAnalysis(ScanAndReScanApplicationJobRequest fastScanRequest, LogPollingProvider logPollingProvider) throws ApplicationServiceException {
        log.info("Starting job to perform Deep Analysis action (Run Analysis) ");
        try {
            String jobGuid = jobService.startDeepAnalysis(fastScanRequest);
            log.info("Deep Analysis running job GUID= " + jobGuid);
            return logPollingProvider != null ? logPollingProvider.pollJobLog(jobGuid) : null;
        } catch (JobServiceException e) {
            log.error("Could not perform the Deep Analysis due to the following error", e);
            throw new ApplicationServiceException("Unable to Run Deep Analysis automatically.", e);
        }
    }

    @Override
    public boolean isOnboardingSettingsEnabled() throws ApplicationServiceException {
        try {
            JsonDto<Boolean> answer = restApiService.getForEntity(ApiEndpointHelper.getEnableOnboardingSettingsEndPoint(), JsonDto.class);
            return answer.getData();
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to retrieve the onboarding mode settings", e);
        }
    }

    @Override
    public boolean isImagingAvailable() throws ApplicationServiceException {
        try {
            ImagingSettingsDto imagingDto = restApiService.getForEntity(ApiEndpointHelper.getImagingSettingsEndPoint(), ImagingSettingsDto.class);
            return imagingDto.isValid();
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to retrieve the onboarding mode settings", e);
        }
    }

    @Override
    public void setEnableOnboarding(boolean enabled) throws ApplicationServiceException {
        try {
            restApiService.putForEntity(ApiEndpointHelper.getEnableOnboardingSettingsEndPoint(), JsonDto.of(enabled), String.class);
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to update the 'On-boarding mode' settings", e);
        }
    }

    @Override
    public ApplicationOnboardingDto getApplicationOnboarding(String applicationGuid) throws ApplicationServiceException {
        try {
            return restApiService.getForEntity(ApiEndpointHelper.getApplicationOnboardingPath(applicationGuid), ApplicationOnboardingDto.class);
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get onboarded application with GUID: " + applicationGuid, e);
        }
    }

    private Applications getApplications() throws ApplicationServiceException {
        try {
            Applications result = restApiService.getForEntity(ApiEndpointHelper.getApplicationsPath(), Applications.class);
            return result == null ? new Applications() : result;
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get applications from AIP Console", e);
        }
    }

    private Set<DomainDto> getDomains() throws ApplicationServiceException {
        try {
            return restApiService.getForEntity(ApiEndpointHelper.getDomainsPath(), new TypeReference<Set<DomainDto>>() {
            });
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get domains from CAST Imaging Console", e);
        }

    }

    @Override
    public Set<VersionDto> getApplicationVersion(String appGuid) throws ApplicationServiceException {
        try {
            return restApiService.getForEntity(ApiEndpointHelper.getApplicationVersionsPath(appGuid), new TypeReference<Set<VersionDto>>() {
            });
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to retrieve the applications' versions", e);
        }
    }

    @Override
    public DebugOptionsDto getDebugOptions(String appGuid) {
        try {
            return restApiService.getForEntity(ApiEndpointHelper.getDebugOptionsPath(appGuid), new TypeReference<DebugOptionsDto>() {
            });
        } catch (ApiCallException e) {
            return DebugOptionsDto.builder().build();
        }
    }

    @Override
    public void updateSecurityDataflow(String appGuid, boolean securityDataflowFlag, String technologyPath) {
        try {
            restApiService.putForEntity(ApiEndpointHelper.getApplicationSecurityDataflowPath(appGuid) + technologyPath, JsonDto.of(securityDataflowFlag), String.class);
        } catch (ApiCallException e) {
            log.warn(e.getMessage());
        }
    }

    @Override
    public void updateShowSqlDebugOption(String appGuid, boolean showSql) {
        try {
            restApiService.putForEntity(ApiEndpointHelper.getDebugOptionShowSqlPath(appGuid), JsonDto.of(showSql), String.class);
        } catch (ApiCallException e) {
            log.warn(e.getMessage());
        }
    }

    @Override
    public void updateAmtProfileDebugOption(String appGuid, boolean amtProfile) {
        try {
            //--------------------------------------------------------------
            //The PUT shouldn't return anything than void.class, but doing so clashed as object mapper is trying to map
            //Some response body. The response interpreter here does behave as expected.
            //Using String.class prevents from type clash (!#?)
            //--------------------------------------------------------------
            restApiService.putForEntity(ApiEndpointHelper.getDebugOptionAmtProfilePath(appGuid), JsonDto.of(amtProfile), String.class);
        } catch (ApiCallException e) {
            //log.error( e.getMessage());
        }
    }

    @Override
    public void resetDebugOptions(String appGuid, DebugOptionsDto debugOptionsDto) {
        updateShowSqlDebugOption(appGuid, debugOptionsDto.isShowSql());
        updateAmtProfileDebugOption(appGuid, debugOptionsDto.isActivateAmtMemoryProfile());
    }

    @Override
    public void setModuleOptionsGenerationType(String appGuid, ModuleGenerationType generationType) {
        //This endpoint operates only with either "one_per_au" or "full_content"
        if (generationType != null && generationType != ModuleGenerationType.ONE_PER_TECHNO) {
            try {
                restApiService.putForEntity(ApiEndpointHelper.getModuleOptionsGenerationTypePath(appGuid), JsonDto.of(generationType.toString()), String.class);
            } catch (ApiCallException e) {
                log.warn(e.getMessage());
            }
        }
    }

    @Override
    public void updateModuleGenerationType(String applicationGuid, JobRequestBuilder builder, ModuleGenerationType moduleGenerationType, boolean firstVersion) {
        if (moduleGenerationType != null && moduleGenerationType != ModuleGenerationType.PRESERVE_CONFIGURED) {
            if (moduleGenerationType == ModuleGenerationType.FULL_CONTENT) {
                setModuleOptionsGenerationType(applicationGuid, moduleGenerationType);
                log.info("Module option has been set to " + moduleGenerationType);
            } else if (firstVersion) {
                //Job will handle it
                builder.moduleGenerationType(moduleGenerationType);
            } else { //clone
                if (moduleGenerationType == ModuleGenerationType.ONE_PER_AU) {
                    setModuleOptionsGenerationType(applicationGuid, moduleGenerationType);
                    log.info("Module option has been set to " + moduleGenerationType);
                } else {
                    //delegated to the job that will issue the appropriate message in case of;
                    builder.moduleGenerationType(moduleGenerationType);
                }
            }
        }
    }

    @Override
    public String discoverPackagesAndCreateDeliveryConfiguration(String appGuid
            , String sourcePath, Exclusions exclusions
            , VersionStatus status, boolean rescan, Consumer<DeliveryConfigurationDto> deliveryConfigConsumer
            , boolean throwPackagePathCheckError) throws JobServiceException {
        ApiInfoDto apiInfoDto = restApiService.getAipConsoleApiInfo();
        String flag = apiInfoDto.isEnablePackagePathCheck() ? "enabled" : "disabled";
        log.info("enable.package.path.check option is " + flag);

        try {
            Set<DeliveryPackageDto> packages = new HashSet<>();
            VersionDto previousVersion = getApplicationVersion(appGuid)
                    .stream()
                    .filter(v -> v.getStatus().ordinal() >= status.ordinal())
                    .max(Comparator.comparing(VersionDto::getVersionDate)).orElse(null);
            Set<String> ignorePatterns = StringUtils.isEmpty(exclusions.getExcludePatterns()) ?
                    Exclusions.getDefaultIgnorePatterns() : Arrays.stream(exclusions.getExcludePatterns().split(",")).collect(Collectors.toSet());

            DeliveryConfigurationDto deliveryConfigurationDto = DeliveryConfigurationDto.builder()
                    .ignorePatterns(ignorePatterns)
                    .exclusionRules(exclusions.getExclusionRules())
                    .packages(packages)
                    .build();
            if (deliveryConfigConsumer != null) {
                deliveryConfigConsumer.accept(deliveryConfigurationDto);
            }
            log.info("Exclusion patterns: " + String.join(", ", deliveryConfigurationDto.getIgnorePatterns()));
            log.info("Project exclusion rules: " + deliveryConfigurationDto.getExclusionRules().stream().map(ExclusionRuleDto::getRule).collect(Collectors.joining(", ")));
            BaseDto response = restApiService.postForEntity("/api/applications/" + appGuid + "/delivery-configuration", deliveryConfigurationDto, BaseDto.class);
            log.debug("Delivery configuration response " + response);
            return response != null ? response.getGuid() : null;
        } catch (ApplicationServiceException | ApiCallException e) {
            log.error("Failed to create the Delivery configuration ");
            throw new JobServiceException("Error creating delivery config", e);
        }
    }

    @Override
    public String reDiscoverApplication(String appGuid, String sourcePath, String versionName, DeliveryConfigurationDto deliveryConfig,
                                        String caipVersion, String targetNode, boolean verbose, LogPollingProvider logPollingProvider) throws ApplicationServiceException {
        try {
            log.info("Starting ReDiscover Application job for application GUID= " + appGuid);
            String jobGuid = jobService.startReDiscoverApplication(appGuid, sourcePath, versionName, deliveryConfig, caipVersion, targetNode);
            log.info("ReDiscover Application running job GUID= " + jobGuid);
            return logPollingProvider.pollJobLog(jobGuid);
        } catch (JobServiceException e) {
            log.error("Could not re-discover application contents due to following error", e);
            throw new ApplicationServiceException("Unable to re-discover application contents automatically.", e);
        }
    }

    @Override
    public String publishToImaging(String appGuid, LogPollingProvider logPollingProvider) throws ApplicationServiceException {
        try {
            log.info("Starting Publish to Imaging job for application GUID= " + appGuid);
            String jobGuid = jobService.startPublishToImaging(appGuid, null, null);
            log.info("Publish to Imaging running job GUID= " + jobGuid);
            return logPollingProvider != null ? logPollingProvider.pollJobLog(jobGuid) : null;
        } catch (JobServiceException e) {
            log.error("Application data could not be Published to Imaging due to following error", e);
            throw new ApplicationServiceException("Unable to Publish application contents to Imaging.", e);
        }
    }

    @Override
    public String createDeliveryConfiguration(String appGuid, String sourcePath, Exclusions exclusions, boolean rescan) throws JobServiceException, PackagePathInvalidException {
        return discoverPackagesAndCreateDeliveryConfiguration(appGuid, sourcePath, exclusions, VersionStatus.DELIVERED, rescan, null, false);
    }

    private Set<DeliveryPackageDto> discoverPackages(String appGuid, String sourcePath, String previousVersionGuid, boolean throwPackagePathCheckError) throws PackagePathInvalidException, JobServiceException {
        try {
            Response resp = restApiService.exchangeForResponse("POST", "/api/applications/" + appGuid + "/delivery-configuration/discover-packages",
                    DiscoverPackageRequest.builder().previousVersionGuid(previousVersionGuid).sourcePath(sourcePath).build());
            int status = resp.code();
            Response packageResponse = null;
            if (status == 200) {
                packageResponse = resp;
            } else if (status == 202) {
                PendingResultDto resultDto = restApiService.mapResponse(resp, PendingResultDto.class);
                while (status != 200) {
                    log.debug("Polling server to get discovered packages...");
                    Response response = restApiService.exchangeForResponse("GET", "/api/applications/" + appGuid + "/pending-results/" + resultDto.getGuid(), null);
                    status = response.code();

                    if (status == 200) {
                        packageResponse = response;
                        break;
                    }
                    Thread.sleep(5000);
                }
            }
            if (packageResponse != null) {
                Set<DeliveryPackageDto> packages = restApiService.mapResponse(packageResponse, new TypeReference<Set<DeliveryPackageDto>>() {
                });

                ApplicationDto app = getApplicationFromGuid(appGuid);
                if ((throwPackagePathCheckError || !app.isInPlaceMode()) && packages.stream().anyMatch(p -> p.getPath() == null)) {
                    throw new PackagePathInvalidException(packages.stream().filter(p -> p.getPath() == null).collect(Collectors.toSet()));
                }
                return packages;
            }
            return Collections.emptySet();
        } catch (ApiCallException | InterruptedException | ApplicationServiceException e) {
            throw new JobServiceException("Error discovering packages", e);
        }
    }

    private List<ApplicationCommonDetailsDto> getApplicationCommonDetails() throws ApplicationServiceException {
        try {
            List<ApplicationCommonDetailsDto> result = restApiService.getForEntity(
                    ApiEndpointHelper.getApplicationsCommonDetailsPath()
                    , new TypeReference<List<ApplicationCommonDetailsDto>>() {});

            return result == null ? null : result;
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get applications list", e);
        }
    }
}
