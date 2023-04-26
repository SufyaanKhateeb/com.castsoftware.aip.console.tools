package com.castsoftware.aip.console.tools.commands;

import com.castsoftware.aip.console.tools.core.dto.ApplicationDto;
import com.castsoftware.aip.console.tools.core.dto.architecturestudio.ArchitectureModelDto;
import com.castsoftware.aip.console.tools.core.services.ApplicationService;
import com.castsoftware.aip.console.tools.core.services.ArchitectureStudioService;
import com.castsoftware.aip.console.tools.core.services.JobsService;
import com.castsoftware.aip.console.tools.core.services.RestApiService;
import com.castsoftware.aip.console.tools.core.services.UploadService;
import com.castsoftware.aip.console.tools.core.utils.Constants;
import com.castsoftware.aip.console.tools.core.utils.VersionInformation;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import java.util.Set;
import java.util.stream.Collectors;

@Component
@CommandLine.Command(
        name = "ArchitectureStudio",
        mixinStandardHelpOptions = true,
        aliases = {"Arch-Studio"},
        description = "Arch studio"
)
@Slf4j
@Getter
@Setter
public class ArchitectureStudioCommand extends BasicCollable{

    @Autowired
    private ArchitectureStudioService archService;
    @CommandLine.Mixin
    private SharedOptions sharedOptions;

    @CommandLine.Option(
            names = {"-n", "--app-name"},
            paramLabel = "APPLICATION_NAME",
            description = "The name of the application",
            required = true)
    private String applicationName;

    @CommandLine.Option(
            names = "--model-name",
            description = "The name of the model",
            required = true)
    private String modelName;

    private static final VersionInformation MIN_VERSION = VersionInformation.fromVersionString("2.8.0");

    public ArchitectureStudioCommand(RestApiService restApiService, JobsService jobsService, UploadService uploadService, ApplicationService applicationService) {
        super(restApiService, jobsService, uploadService, applicationService);
    }

    @Override
    public Integer processCallCommand() throws Exception {
        if (StringUtils.isBlank(modelName)) {
            log.error("Model name should not be empty.");
            return Constants.RETURN_APPLICATION_INFO_MISSING;
        }
        log.info(String.format("Getting model: %s", modelName));
        Set<ArchitectureModelDto> modelDtoSet = archService.getArchitectureModels();

        /* Search name of the model in the list of available models and get the model details. */
        ArchitectureModelDto modelInUse = modelDtoSet.stream().filter(m -> m.getModelName().equalsIgnoreCase(modelName)).collect(Collectors.collectingAndThen(Collectors.toSet(),
                (Set<ArchitectureModelDto> dtoSet) -> dtoSet.isEmpty() ? null : dtoSet.iterator().next()));

        //Check if model list is empty
        if (modelInUse == null){
            log.error(String.format("Architecture model '{}' could not be found.", modelName));
            return Constants.RETURN_ARCHITECTURE_MODEL_NOT_FOUND;
        }

        ApplicationDto app = applicationService.getApplicationFromName(applicationName);
        if (app == null){
            log.error(String.format("Application '{}' could not be found.", applicationName));
            return Constants.RETURN_APPLICATION_NOT_FOUND;
        }

        String path = modelInUse.getPath();
        log.info("App '{}' successful", applicationName);
        log.info(path);
        return Constants.RETURN_OK;
    }

    @Override
    protected VersionInformation getMinVersion() {
        return MIN_VERSION;
    }

    @Override
    public SharedOptions getSharedOptions() {
        return sharedOptions;
    }
}
