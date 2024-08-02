package com.castsoftware.aip.console.tools.commands;

import com.castsoftware.aip.console.tools.providers.VersionProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

/**
 * This class represent a "main" command that will then delegate to subcommands
 * based on passed COMMAND parameter
 */
@Component
@Command(
        name = "aip-integration-tool",
        mixinStandardHelpOptions = true,
        subcommands = {
                FastScanCommand.class,
                DeepAnalysisCommand.class,
                PublishToImagingCommand.class,
                OnboardApplicationCommand.class,
                ImportApplicationsCommand.class
        },
        commandListHeading = "%nPossible values for COMMAND:%n%n",
        versionProvider = VersionProvider.class
)
@Getter
@Setter
@Slf4j
public class ParentCommand implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        log.error("No COMMAND provided");
        CommandLine.usage(this, System.out);
        return 0;
    }
}
