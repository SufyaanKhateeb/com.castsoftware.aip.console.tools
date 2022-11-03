package com.castsoftware.aip.console.tools.core.utils;

import com.castsoftware.aip.console.tools.core.dto.jobs.CreateJobsRequest;
import com.castsoftware.aip.console.tools.core.exceptions.JobServiceException;
import org.apache.commons.lang3.StringUtils;

public class ApiEndpointHelper {

    private static final String ROOT_PATH = "/api";
    private static final String URL_SETTINGS_PATH = ROOT_PATH + "/settings";
    private static final String APPLICATIONS_ENDPOINT = "/applications";
    private static final String JOBS_ENDPOINT = "/jobs";
    private static final String UPLOAD_ENDPOINT = "/upload";
    private static final String VERSIONS_ENDPOINT = "/versions";
    private static final String EXTRACT_ENDPOINT = "/extract";
    private static final String DEBUG_OPTIONS_ENDPOINT = "/debug-options";
    private static final String SHOW_SQL_ENDPOINT = "/show-sql";
    private static final String AMT_PROFILE_ENDPOINT = "/activate-amt-memory-profile";
    private static final String CREATE_APPLICATION_ENDPOINT = "/create-application";
    public static final String ADD_VERSION_ENDPOINT = "/add-version";
    public static final String CLONE_VERSION_ENDPOINT = "/clone-version";
    public static final String ANALYZE_SECURITY_DATAFLOW = "/analyze-security-dataflow";
    public static final String RESYNC_APPLICATION = "/resync-application";
    public static final String SHERLOCK_BACKUP_ENDPOINT = "/sherlock-backup";
    public static final String RESTORE_ENDPOINT = "/restore";
    public static final String BACKUP_ENDPOINT = "/backup";
    public static final String REJECT_VERSION_ENDPOINT = "/reject-version";
    public static final String DELETE_VERSION_ENDPOINT = "/delete-version";
    public static final String PURGE_VERSION_ENDPOINT = "/purge-version";
    public static final String CONSOLIDATE_SNAPSHOT_ENDPOINT = "/consolidate-snapshot";
    public static final String ANALYZE_ENDPOINT = "/analyze";
    public static final String MODULE_OPTIONS_ENDPOINT = "/module-options";

    public static final String MODULE_OPTIONS_GENERATION_TYPE_ENDPOINT = MODULE_OPTIONS_ENDPOINT + "/generation-type";
    public static final String URL_AIC_PATH = "/aic";
    public static final String URL_AIC_DOMAINS_PATH = URL_AIC_PATH + "/domains";
    public static final String URL_ONBOARDING_PATH = "/onboarding";
    public static final String ONBOARD_APPLICATION_ENDPOINT = "/onboard-application";
    public static final String DISCOVER_APPLICATION_ENDPOINT = "/discover-application";
    public static final String ENABLE_ONBOARDING_PATH = "/enable-onboarding";
    public static final String IMAGING_SETTINGS_PATH = "/imaging-settings";
    public static final String FIRST_SCAN_PATH = "/first-scan-application";

    public static String getRootPath() {
        return ROOT_PATH + "/";
    }

    public static String getApplicationsPath() {
        return ROOT_PATH + APPLICATIONS_ENDPOINT;
    }

    public static String getDomainsPath() {
        return ROOT_PATH + URL_AIC_DOMAINS_PATH;
    }

    public static String getApplicationPath(String appGuid) {
        assert appGuid != null && !appGuid.isEmpty();

        return getApplicationsPath() + "/" + appGuid;
    }

    public static String getApplicationVersionsPath(String appGuid) {
        assert appGuid != null && !appGuid.isEmpty();
        return getApplicationPath(appGuid) + VERSIONS_ENDPOINT;
    }

    public static String getApplicationCreateUploadPath(String appGuid) {
        assert appGuid != null && !appGuid.isEmpty();

        return getApplicationPath(appGuid) + UPLOAD_ENDPOINT;
    }

    public static String getApplicationOnboardingUploadPath() {
        return getApplicationsPath() + URL_ONBOARDING_PATH + UPLOAD_ENDPOINT;
    }

    public static String getRefreshContentsUploadPath(String appGuid) {
        return getApplicationsPath() + "/" + appGuid + UPLOAD_ENDPOINT;
    }

    public static String getApplicationOnboardingPath(String appGuid) {
        return getApplicationPath(appGuid) + URL_ONBOARDING_PATH;
    }

    public static String getApplicationOnboardingUploadChunkPath(String applicationGuid, String uploadGuid) {
        assert uploadGuid != null && !uploadGuid.isEmpty();
        return (StringUtils.isNotEmpty(applicationGuid) ? getApplicationCreateUploadPath(applicationGuid) : getApplicationOnboardingUploadPath()) + "/" + uploadGuid;
    }

    public static String getOnboardApplicationEndPoint() {
        return getJobsEndpoint() + ONBOARD_APPLICATION_ENDPOINT;
    }

    public static String getFirstScanEndPoint() {
        return getJobsEndpoint() + FIRST_SCAN_PATH;
    }

    public static String getDiscoverApplicationEndPoint() {
        return getJobsEndpoint() + DISCOVER_APPLICATION_ENDPOINT;
    }

    public static String getEnableOnboardingSettingsEndPoint() {
        return URL_SETTINGS_PATH + ENABLE_ONBOARDING_PATH;
    }

    public static String getImagingSettingsEndPoint() {
        return URL_SETTINGS_PATH + IMAGING_SETTINGS_PATH;
    }

    public static String getApplicationUploadPath(String appGuid, String uploadGuid) {
        assert appGuid != null && !appGuid.isEmpty();
        assert uploadGuid != null && !uploadGuid.isEmpty();

        return getApplicationCreateUploadPath(appGuid) + "/" + uploadGuid;
    }

    public static String getApplicationExtractUploadPath(String appGuid, String uploadGuid) {
        return getApplicationUploadPath(appGuid, uploadGuid) + EXTRACT_ENDPOINT;
    }

    public static String getDebugOptionsPath(String appGuid) {
        return getApplicationPath(appGuid) + DEBUG_OPTIONS_ENDPOINT;
    }

    public static String getDebugOptionShowSqlPath(String appGuid) {
        return getDebugOptionsPath(appGuid) + SHOW_SQL_ENDPOINT;
    }

    public static String getDebugOptionAmtProfilePath(String appGuid) {
        return getDebugOptionsPath(appGuid) + AMT_PROFILE_ENDPOINT;
    }

    public static String getModuleOptionsGenerationTypePath(String appGuid) {
        return getApplicationPath(appGuid) + MODULE_OPTIONS_GENERATION_TYPE_ENDPOINT;
    }

    public static String getAmtProfilingDownloadUrl(String appGuid) {
        return ApiEndpointHelper.getDebugOptionsPath(appGuid) + "/logs" + AMT_PROFILE_ENDPOINT + "/download";
    }

    public static String getJobsEndpoint() {
        return ROOT_PATH + JOBS_ENDPOINT;
    }

    public static String getCreateApplicationEndPoint() {
        return getJobsEndpoint() + CREATE_APPLICATION_ENDPOINT;
    }

    public static String getJobsEndpoint(CreateJobsRequest jobRequest) throws JobServiceException {
        switch (jobRequest.getJobType()) {
            case ADD_VERSION:
                return getJobsEndpoint() + ADD_VERSION_ENDPOINT;
            case CLONE_VERSION:
                return getJobsEndpoint() + CLONE_VERSION_ENDPOINT;
            case DATAFLOW_SECURITY_ANALYZE:
                return getJobsEndpoint() + ANALYZE_SECURITY_DATAFLOW;
            case RESCAN_APPLICATION:
                return getJobsEndpoint() + RESYNC_APPLICATION;
            case UPLOAD_SNAPSHOT_VERSION:
                return getJobsEndpoint() + RESYNC_APPLICATION;
            case SHERLOCK_BACKUP:
                return getJobsEndpoint() + SHERLOCK_BACKUP_ENDPOINT;
            case RESTORE:
                return getJobsEndpoint() + RESTORE_ENDPOINT;
            case BACKUP:
                return getJobsEndpoint() + BACKUP_ENDPOINT;
            case REJECT_VERSION:
                return getJobsEndpoint() + REJECT_VERSION_ENDPOINT;
            case PURGE_VERSION:
                return getJobsEndpoint() + PURGE_VERSION_ENDPOINT;
            case DELETE_VERSION:
                return getJobsEndpoint() + DELETE_VERSION_ENDPOINT;
            case CONSOLIDATE_SNAPSHOT:
                return getJobsEndpoint() + CONSOLIDATE_SNAPSHOT_ENDPOINT;
            case ANALYZE:
                return getJobsEndpoint() + ANALYZE_ENDPOINT;
        }
        throw new JobServiceException("Undefined AIP Console job endpoint: " + jobRequest.getJobType().toString());
    }

    public static String getJobDetailsEndpoint(String jobGuid) {
        assert jobGuid != null && !jobGuid.isEmpty();

        return getJobsEndpoint() + "/" + jobGuid;
    }
}
