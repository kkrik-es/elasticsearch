/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Request for whether system features need to be upgraded
 */
public class GetFeatureUpgradeStatusRequest extends MasterNodeRequest<GetFeatureUpgradeStatusRequest> {

    public GetFeatureUpgradeStatusRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    public GetFeatureUpgradeStatusRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
