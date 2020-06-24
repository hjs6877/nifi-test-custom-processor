package com.pentasecurity.processors;

import com.pentasecurity.callback.ReverseStringCallback;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"text", "test"})
@CapabilityDescription("You can reverse input text using this processor.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="reverse", description="notify a result if an input text reversed or not.")})
public class ReverseStringProcessor extends AbstractProcessor {
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    public static final Relationship ERROR_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Error relationship")
            .build();

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = Collections.emptyList();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(ERROR_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            ReverseStringCallback processorCallback = new ReverseStringCallback();
            session.write(flowFile, processorCallback);

            if (processorCallback.isSuccess()) {
                session.putAttribute(flowFile, "reversed", "true");
                session.transfer(flowFile, SUCCESS_RELATIONSHIP);
            } else {
                session.transfer(flowFile, ERROR_RELATIONSHIP);
            }
        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
        }


    }
}
