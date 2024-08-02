package com.castsoftware.aip.console.tools.commands;

import com.castsoftware.aip.console.tools.core.dto.DatabaseConnectionSettingsDto;
import com.castsoftware.aip.console.tools.core.dto.ImportApplicationDto;
import com.castsoftware.aip.console.tools.core.exceptions.ApiCallException;
import com.castsoftware.aip.console.tools.core.exceptions.ApplicationServiceException;
import com.castsoftware.aip.console.tools.core.services.ApplicationService;
import com.castsoftware.aip.console.tools.core.services.JobsService;
import com.castsoftware.aip.console.tools.core.services.RestApiService;
import com.castsoftware.aip.console.tools.core.services.UploadService;
import com.castsoftware.aip.console.tools.core.utils.ApiEndpointHelper;
import com.castsoftware.aip.console.tools.core.utils.VersionInformation;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import java.util.List;

@Component
@CommandLine.Command(
        name = "Import Applications List",
        mixinStandardHelpOptions = true,
        aliases = {"ls"},
        description = "List the applications that can be imported."
)
@Slf4j
@Getter
@Setter
public class ListImportApplications extends BasicCallable {

    @CommandLine.Mixin
    private SharedOptions sharedOptions;

    //This version can be null if failed to convert from string
    private static final VersionInformation MIN_VERSION = VersionInformation.fromVersionString("3.0.0");

    protected ListImportApplications(RestApiService restApiService, JobsService jobsService, UploadService uploadService, ApplicationService applicationService) {
        super(restApiService, jobsService, uploadService, applicationService);
    }

    private List<DatabaseConnectionSettingsDto> getCssServers() throws ApplicationServiceException {
        try {
            return restApiService.getForEntity( ApiEndpointHelper.getCssSettings(), new TypeReference<List<DatabaseConnectionSettingsDto>>() {});
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get css server list", e);
        }
    }

    private List<ImportApplicationDto> getServerAppList(String serverGuid) throws ApplicationServiceException {
        try {
            return restApiService.getForEntity( ApiEndpointHelper.getServerAppList(serverGuid), new TypeReference<List<ImportApplicationDto>>() {});
        } catch (ApiCallException e) {
            throw new ApplicationServiceException("Unable to get css server list", e);
        }
    }
    @Override
    public Integer processCallCommand() throws Exception {
        List<DatabaseConnectionSettingsDto> cssServers = getCssServers();
        for (DatabaseConnectionSettingsDto cssServer: cssServers) {
            System.out.println("{");
            System.out.println("\tCSS GUID: " + cssServer.getGuid());
            System.out.println("\tCSS Name: " + cssServer.getDatabaseName());
            List<ImportApplicationDto> serverAppList = getServerAppList(cssServer.getGuid());
            System.out.println("\tImportable applications: [");
            for(int i = 1; i <= serverAppList.size(); i++) {
                ImportApplicationDto app = serverAppList.get(i-1);
                System.out.println("\t\t" + app.toString() + (i != serverAppList.size() ? ',' : ' '));
            }
            System.out.println("\t]");
            System.out.println("}");
        }
        return 0;
    }

    @Override
    protected VersionInformation getMinVersion() { return MIN_VERSION; }
}
