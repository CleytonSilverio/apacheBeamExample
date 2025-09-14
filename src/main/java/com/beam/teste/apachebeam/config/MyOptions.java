package com.beam.teste.apachebeam.config;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions{

	@Description("DB URL")
    String getDbUrl();
    void setDbUrl(String value);

    @Description("DB Username")
    String getDbUsername();
    void setDbUsername(String value);

    @Description("DB Password")
    String getDbPassword();
    void setDbPassword(String value);
	
}
