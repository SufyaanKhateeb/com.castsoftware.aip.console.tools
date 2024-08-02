package com.castsoftware.aip.console.tools.commands;

import com.castsoftware.aip.console.tools.core.dto.DatabaseConnectionSettingsDto;
import com.castsoftware.aip.console.tools.core.dto.ImportApplicationDto;
import com.castsoftware.aip.console.tools.core.dto.PendingResultDto;
import com.castsoftware.aip.console.tools.core.exceptions.ApiCallException;
import com.castsoftware.aip.console.tools.core.exceptions.ApplicationServiceException;
import com.castsoftware.aip.console.tools.core.services.ApplicationService;
import com.castsoftware.aip.console.tools.core.services.JobsService;
import com.castsoftware.aip.console.tools.core.services.RestApiService;
import com.castsoftware.aip.console.tools.core.services.UploadService;
import com.castsoftware.aip.console.tools.core.utils.ApiEndpointHelper;
import com.castsoftware.aip.console.tools.core.utils.VersionInformation;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@CommandLine.Command(
        name = "Import Application",
        mixinStandardHelpOptions = true,
        aliases = {"Import-App"},
        //edit description to match v3
        description = "Imports applications from other nodes",
        subcommands = ListImportApplications.class
)
@Slf4j
@Getter
@Setter
public class ImportApplicationsCommand extends BasicCallable {

    @CommandLine.Option(names = {"-apps", "--app-names"},
            paramLabel = "APPLICATION_NAMES_LIST",
            description = "The list of the application names to import",
            split = ",")
    private String[] applicationListOption = {};

//    @CommandLine.Option(names = {"-css", "--css-servers"},
//            paramLabel = "CSS_SERVER_GUID_LIST",

//            description = "The list of guids of the css servers to import from",
//            split = ",")
//    private String[] cssServerGuidListOption = {};

    @CommandLine.Option(names = {"-a", "--all"},
            paramLabel = "IMPORT_ALL",
            description = "If css server guids are specified then import all the applications in those servers. If --css-servers option is not provided then, import all the applications available for import."
            + " If this option is specified then the --app-guids will not be used.",
            defaultValue = "false", fallbackValue = "true")
    private Boolean importAll;

    @CommandLine.Mixin
    private SharedOptions sharedOptions;

    //This version can be null if failed to convert from string
    private static final VersionInformation MIN_VERSION = VersionInformation.fromVersionString("3.0.0");

    public ImportApplicationsCommand(RestApiService restApiService, JobsService jobsService, UploadService uploadService, ApplicationService applicationService) {
        super(restApiService, jobsService, uploadService, applicationService);
    }

    @Override
    public Integer processCallCommand() throws Exception {
        // convert Arrays to Lists
        List<String> applicationList = Arrays.asList(applicationListOption);

        // Remove duplicates from applicationList
        Set<String> uniqueApplications = new HashSet<>(applicationList);
        applicationList = new ArrayList<>(uniqueApplications);

        if (applicationList.isEmpty() && !importAll) {
            log.error("Atleast one application name or import all flag should be specified for importing.");
            return 0;
        }

        List<DatabaseConnectionSettingsDto> cssServers = getCssServers();

        Map<String, List<ImportApplicationDto>> serverAppListMap = new HashMap<>();

        // If importAll flag is not set to true, validate if all application names provided are present and create server to applications mapping
        if(!importAll) {
            long totalCount = 0;
            for (DatabaseConnectionSettingsDto cssServer : cssServers) {
                List<ImportApplicationDto> serverAppList = getServerAppList(cssServer.getGuid());

                totalCount += getAppPresentCount(serverAppList, applicationList);
                List<String> finalApplicationList = applicationList;
                serverAppListMap.put(cssServer.getGuid(), serverAppList.stream().filter(app -> finalApplicationList.contains(app.getAppName())).collect(Collectors.toList()));
            }
            if(totalCount != applicationList.size()) {
                log.error("Invalid application names provided.");
                return 0;
            }
        } else {
            for (DatabaseConnectionSettingsDto cssServer : cssServers) {
                List<ImportApplicationDto> serverAppList = getServerAppList(cssServer.getGuid());
                serverAppListMap.put(cssServer.getGuid(), serverAppList);
            }
        }

        importApps(serverAppListMap);

        return 0;
    }

    @Override
    protected VersionInformation getMinVersion() { return MIN_VERSION; }

    private List<DatabaseConnectionSettingsDto> getCssServers() throws ApplicationServiceException {
        try {
            return restApiService.getForEntity(ApiEndpointHelper.getCssSettings(), new TypeReference<List<DatabaseConnectionSettingsDto>>() {});
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get css server list", e);
        }
    }

    private List<ImportApplicationDto> getServerAppList(String serverGuid) throws ApplicationServiceException {
        try {
            return restApiService.getForEntity(ApiEndpointHelper.getServerAppList(serverGuid), new TypeReference<List<ImportApplicationDto>>() {});
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get css server list", e);
        }
    }

    private boolean areAllCssGuidsPresent(List<DatabaseConnectionSettingsDto> cssServers, List<String> selectedGuids) {
        // Convert the list of CSS servers to a set of GUIDs for faster lookup
        Set<String> guidSet = cssServers.stream().map(DatabaseConnectionSettingsDto::getGuid).collect(Collectors.toSet());

        // Check if all selected GUIDs are present in the set of GUIDs from CSS servers
        return guidSet.containsAll(selectedGuids);
    }

    private long getAppPresentCount(List<ImportApplicationDto> serverApplications, List<String> selectedAppNames) {
        return serverApplications.stream()
                .filter(app -> selectedAppNames.contains(app.getAppName()))
                .count();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ImportedApplicationResponse {
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class ImportedApplicationError {
            private String code;
            private String defaultMessage;
        }
        private String appName;
        private boolean imported;
        private ImportedApplicationError error = null;
    }

    public class importRequest {
        public List<ImportApplicationDto> data;
        public importRequest(List<ImportApplicationDto> data) {
            this.data = data;
        }
    }
    private void importApps(Map<String, List<ImportApplicationDto>> serverAppListMap) throws ApiCallException {
        for(String serverGuid: serverAppListMap.keySet()) {
            try {
                List<ImportApplicationDto> appsToImport = serverAppListMap.get(serverGuid);
                Response resp = restApiService.exchangeForResponse("POST", ApiEndpointHelper.getImportApplication(serverGuid), new importRequest(appsToImport));

                int status = resp.code();
                Response packageResponse = resp;
                while (status == 202) {
                    PendingResultDto resultDto = restApiService.mapResponse(resp, PendingResultDto.class);
                    log.debug("Polling server to get discovered packages...");
                    resp = restApiService.exchangeForResponse("GET", "/api/pending-results/" + resultDto.getGuid(), null);
                    status = resp.code();

                    if (status == 200) {
                        packageResponse = resp;
                        break;
                    }
                    Thread.sleep(2500);
                }

                if(status == 200) {
                    List<ImportedApplicationResponse> responseData = restApiService.mapResponse(packageResponse,  new TypeReference<List<ImportedApplicationResponse>>() {});
                    if(!responseData.isEmpty()) {
                        log.info("================================================================================");
                        log.info("From css server guid: " + serverGuid);
                    }
                    for(ImportedApplicationResponse importedApp: responseData) {
                        if(importedApp.imported) {
                            log.info("Application \"" + importedApp.getAppName() + "\" was successfully imported.");
                        } else {
                            log.info("Application \"" + importedApp.getAppName() + "\"k was not imported.");
                            if(importedApp.getError() != null) {
                                ImportedApplicationResponse.ImportedApplicationError err = importedApp.getError();
                                log.error("Error code: " + err.getCode());
                                log.error("Error message: " + err.getDefaultMessage());
                            }
                        }
                    }
                    if(!responseData.isEmpty()) log.info("================================================================================");
                }
            } catch (ApiCallException e) {
                log.error("Something went wrong");
                throw new Error(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Error e) {
                System.out.println(e.toString());
            }
        }
    }
}
