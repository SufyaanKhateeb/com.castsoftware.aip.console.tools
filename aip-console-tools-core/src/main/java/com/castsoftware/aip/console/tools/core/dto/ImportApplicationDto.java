package com.castsoftware.aip.console.tools.core.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
public class ImportApplicationDto {
    private String appName;
    private String mngtSchemaName;
    public String toString() {
        return "{ appName: '" + this.appName + "', mngtSchemaName: '" + this.mngtSchemaName + "' }";
    }
}
