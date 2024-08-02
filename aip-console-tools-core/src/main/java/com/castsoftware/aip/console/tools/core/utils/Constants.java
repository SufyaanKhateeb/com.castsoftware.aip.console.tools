package com.castsoftware.aip.console.tools.core.utils;

import java.util.regex.Pattern;

public class Constants {

    private Constants() {
        // NOP
    }

    // Return Codes
    public static final int RETURN_OK = 0;
    public static final int RETURN_NO_PASSWORD = 1;
    public static final int RETURN_LOGIN_ERROR = 2;
    public static final int RETURN_UPLOAD_ERROR = 3;
    public static final int RETURN_JOB_POLL_ERROR = 4;
    public static final int RETURN_JOB_FAILED = 5;
    public static final int RETURN_APPLICATION_INFO_MISSING = 6;
    public static final int RETURN_APPLICATION_NOT_FOUND = 7;
    public static final int RETURN_SOURCE_FOLDER_NOT_FOUND = 8;
    public static final int RETURN_APPLICATION_NO_VERSION = 9;
    public static final int RETURN_APPLICATION_VERSION_NOT_FOUND = 10;
    // Keep ordinal to 9 for backward compatibility
    public static final int RETURN_VERSION_WITH_ANALYSIS_DONE_NOT_FOUND = 9;

    public static final int RETURN_IMPORT_SETTINGS_ERROR = 15;
    public static final int RETURN_INVALID_PARAMETERS_ERROR = 16;
    public static final int RETURN_MISSING_FILE = 17;
    public static final int RETURN_JOB_CANCELED = 18;
    public static final int RETURN_RUN_ANALYSIS_DISABLED = 19;
    public static final int RETURN_SERVER_VERSION_NOT_COMPATIBLE = 20;
    public static final int RETURN_ONBOARD_APPLICATION_DISABLED = 21;
    public static final int RETURN_ONBOARD_VERSION_STATUS_INVALID = 22;
    public static final int RETURN_ONBOARD_OPERATION_FAILED = 23;
    public static final int RETURN_ONBOARD_FAST_SCAN_REQUIRED = 24;
    public static final int RETURN_ONBOARD_FAST_SCAN_FORBIDDEN = 25;
    public static final int RETURN_ONBOARD_DEEP_ANALYSIS_FORBIDDEN = 26;

    public static final int RETURN_ARCHITECTURE_MODEL_NOT_FOUND = 27;
    public static final int RETURN_BAD_SERVER_VERSION = 28;

    public static final String DEFAULT_DOMAIN = "-";

    public static final int UNKNOWN_ERROR = 1000;

    // Utils
    public static final Pattern AIP_VERSION_PATTERN = Pattern.compile("^(1\\.9\\.1-SNAPSHOT)|(1\\.[6-9]\\.\\d)");
    public static final String API_KEY_HEADER = "X-API-KEY";
    public static final String[] ALLOWED_ARCHIVE_EXTENSIONS = new String[]{"zip", "tgz", "gz", "tar.gz"};

    // Job names
    public static final String ANALYZE = "analyze";
    public static final String ONBOARD_APPLICATON = "onboard-application";
    public static final String DISCOVER_APPLICATON = "discover-application";
    public static final String REDISCOVER_APPLICATON = "rediscover-application";
    public static final String FAST_SCAN = "fast-scan";
    public static final String UPDATE_ANALYSIS_DATA = "update-analysis-data";
    public static final String CLEANUP_VERSIONS = "cleanup-versions";
    public static final String CLEANUP_SNAPSHOTS = "cleanup-snapshots";
    public static final String DELETE_SELECTED_SNAPSHOTS = "delete-selected-snapshots";
    public static final String EDIT_VERSION = "edit-version";
    public static final String OPTIMIZE = "optimize";
    public static final String OPTIMIZE_MEASUREMENT = "optimize-measurement";
    public static final String BACKUP_MEASUREMENT = "backup-measurement";
    public static final String UPGRADE_APPLICATION = "upgrade-application";
    public static final String FIRST_SCAN_APPLICATION = "first-scan-application";
    public static final String RESCAN_APPLICATON = "rescan-application";
    public static final String REFRESH_ONBOARDING_DELIVERY_CONFIGURATION = "refresh-onboarding-delivery-configuration";

    public static final String RENAME_SNAPSHOT = "rename-snapshot";
    public static final String REFERENCE_FINDER = "reference_finder";
    public static final String RECOMPUTE_SNAPSHOT_INDICATORS = "recompute-snapshot-indicators";
    public static final String RECOMPUTE_CHECKSUMS = "recompute-checksums";
    public static final String ANALYZE_EXEC_GROUP = "analyze-exec-group";
    public static final String RUN_ANALYZE_UNITS = ANALYZE_EXEC_GROUP;
    public static final String ANALYZE_SECURITY_DATAFLOW = "analyze-security-dataflow";
    public static final String RESYNC_APPLICATION = "resync-application";
    public static final String SYNC_APPLICATION = RESYNC_APPLICATION;
    public static final String RENAME_APPLICATION = "rename-application";

    public static final String PUBLISH_ALL = "publish-all";
    public static final String PUBLISH = "publish";
    public static final String PUBLISH_APPLICATION = PUBLISH;
    public static final String UPLOAD_APPLICATION = PUBLISH;
    public static final String UPDATE_ASSESSMENT_MODEL = "update-assessment-model";
    public static final String INSTALL_EXTENSIONS = "install-extensions";
    public static final String UPDATE_EXTENSIONS = INSTALL_EXTENSIONS;

    public static final String REJECT_VERSION = "reject_version";
    public static final String DELETE_VERSION = "delete_version";
    public static final String PURGE_VERSION = "purge_version";
    public static final String ADD_VERSION = "add_version";
    public static final String CLONE_VERSION = "clone_version";

    public static final String DELETE_SNAPSHOT = "delete_snapshot";
    public static final String CONSOLIDATE_SNAPSHOT = "consolidate_snapshot";
    public static final String SNAPSHOT_INDICATOR = "snapshot_indicator";
    public static final String PROCESS_IMAGING = "process_imaging";
    public static final String DELETE_APPLICATION_FROM_UNIFIED = "delete_application_from_unified";
    public static final String IMPORT_APPLICATION_TO_IMAGING_VIEWER = "import_application_to_imaging_viewer";

    public static final String UPLOAD_SNAPSHOTS = "upload_snapshots";
    // New name for the last step in analysis (this consolidate snapshots and upload to central db)
    public static final String UPLOAD_APP_SNAPSHOT = "upload_application";
    public static final String CREATE_APPLICATION = "create-application";
    public static final String DECLARE_APPLICATION = CREATE_APPLICATION;

    public static final String DELETE_APPLICATION = "delete_application";
    public static final String FUNCTION_POINTS = "function_points";
    public static final String BACKUP = "BACKUP";
    public static final String RESTORE = "RESTORE";
    public static final String SHERLOCK_BACKUP = "SHERLOCK_BACKUP";
    public static final String DELIVER_VERSION = "deliver_version";
    public static final String UPLOAD_DELIVER_VERSION = "upload_deliver_version";
    public static final String RESCAN_APPLICATION = "rescan_application";
    public static final String DEEP_ANALYSIS = "deep-analysis";
    public static final String UPLOAD_SNAPSHOT_VERSION = "upload_snapshot_version";
    public static final String DATAFLOW_SECURITY_ANALYZE = "dataflow_security_analyze";
    public static final String JEE_TECHNOLOGY_PATH = "/jee";
    public static final String DOTNET_TECHNOLOGY_PATH = "/dotnet";
    // Job params
    public static final String PARAM_APP_NAME = "appName";
    public static final String PARAM_INPLACE_MODE = "inPlaceMode";
    public static final String PARAM_DOMAIN_NAME = "domainName";
    public static final String PARAM_VERSION_NAME = "versionName";
    public static final String PARAM_SNAPSHOT_CAPTURE_DATE = "snapshotCaptureDate";
    public static final String PARAM_START_STEP = "startStep";
    public static final String PARAM_END_STEP = "endStep";
    public static final String PARAM_APP_GUID = "appGuid";
    public static final String PARAM_SUSPEND = "suspend";
    public static final String PARAM_IGNORE_CHECK = "ignoreFileStructureCheck";
    public static final String PARAM_RELEASE_DATE = "releaseDate";
    public static final String PARAM_SOURCE_ARCHIVE = "sourceArchive";
    public static final String PARAM_SOURCE_FOLDER = "sourceFolder";
    public static final String PARAM_FILENAME = "fileName";
    public static final String PARAM_VERSION_OBJECTIVES = "objectives";
    public static final String PARAM_SOURCE_PATH = "sourcePath";
    public static final String PARAM_BACKUP_ENABLED = "backupApplication";
    public static final String PARAM_BACKUP_NAME = "backupName";
    public static final String PARAM_VERSION_GUID = "versionGuid";
    public static final String PARAM_SNAPSHOT_NAME = "snapshotName";
    public static final String PARAM_DELIVERY_CONFIG_GUID = "deliveryConfigGuid";
    public static final String PARAM_ENABLE_AUTO_DISCOVER = "extensionAutoConfigEnabled";
    public static final String PARAM_PROCESS_IMAGING = "processImaging";
    public static final String PARAM_UPLOAD_APPLICATION = "uploadApplication";
    public static final String PARAM_MODULE_GENERATION_TYPE = "moduleGenerationType";
    public static final String PARAM_TARGET_NODE = "targetNode";

    // CAIP header name for console v2
    public static final String PARAM_CAIP_VERSION = "X-SC-LB-CAIP";

    // Job Step Names
    public static final String EXTRACT_STEP_NAME = "unzip_source";
    public static final String CODE_SCANNER_STEP_NAME = "code_scanner";
    public static final String SET_CURRENT_STEP_NAME = "setcurrent";
    public static final String ACCEPTANCE_STEP_NAME = "accept";
    public static final String ANALYSIS_STEP_NAME = "analyze";
    public static final String SNAPSHOT_STEP_NAME = "snapshot";
    public static final String CONSOLIDATE_STEP_NAME = "consolidate_snapshot";


    // Other constants
    public static final long DEFAULT_HTTP_TIMEOUT = 90L;
    public static final long DEFAULT_SLEEP_DURATION = 15L; //(15s)
    public static final String EXECUTION_PROFILE_DEFAULT = "default";
    public static final String PARAM_X_API_KEY = "X-API-KEY";
}

