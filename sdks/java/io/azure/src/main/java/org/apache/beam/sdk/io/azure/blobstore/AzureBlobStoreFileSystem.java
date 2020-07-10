/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.azure.blobstore;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.azure.options.AzfsOptions;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;

class AzureBlobStoreFileSystem extends FileSystem<AzfsResourceId> {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreFileSystem.class);

    private final AzfsOptions options;

    AzureBlobStoreFileSystem(AzfsOptions options) {
        this.options = checkNotNull(options, "options");
        // may want to do other things here too
    }

    @Override
    protected String getScheme() {
        return AzfsResourceId.SCHEME;
    }

    @Override
    protected List<MatchResult> match(List<String> specs) throws IOException {

        List<AzfsResourceId> paths = toAzfsPath(specs);

        List<AzfsResourceId> globs = new ArrayList<>();
        List<AzfsResourceId> nonGlobs = new ArrayList<>();
        List<Boolean> isGlobBooleans = new ArrayList<>();

        for (AsfsResourceId path : paths) {
            if (path.isWildcard()) {
                globs.add(path);
                isGlobBooleans.add(true);
            } else {
                nonGlobs.add(path);
                isGlobBooleans.add(false);
            }
        }

        Iterator<MatchResult> globMatches = matchGlobPaths(globs).iterator();
        Iterator<MatchResult> nonGlobMatches = matchNonGlobPaths(nonGlobs).iterator();

        ImmutableList.Builder<MatchResult> matchResults = ImmutableList.builder();
        for (Boolean isGlob : isGlobBooleans) {
            if (isGlob) {
                checkState(globMatches.hasNext(), "Expect globMatches has next.");
                matchResults.add(globMatches.next());
            } else {
                checkState(nonGlobMatches.hasNext(), "Expect nonGlobMatches has next.");
                matchResults.add(nonGlobMatches.next());
            }
        }
        checkState(!globMatches.hasNext(), "Expect no more elements in globMatches.");
        checkState(!nonGlobMatches.hasNext(), "Expect no more elements in nonGlobMatches.");

        return matchResults.build();
    }

    private List<AzfsResourceId> toAzfsPaths(Collection<String> specs) {
        // TODO
        return null;
    }

    @VisibleForTesting
    List<MatchResult> matchGlobPaths(Collection<AzfsResourceId> globPaths) throws IOException {
        // TODO
        return null;
    }

    @VisibleForTesting
    List<MatchResult> matchNonGlobPaths(Collection<AzfsResourceId> globPaths) throws IOException {
        // TODO
        return null;
    }

    @Override
    protected WritableByteChannel create(AzfsResourceId resourceId, CreateOptions createOptions)
            throws IOException {
        // TODO
        return null;
    }

    @Override
    protected ReadableByteChannel open(AzfsResourceId resourceId) throws IOException {
        // TODO
        return null;
    }

    @Override
    protected void copy(List<AzfsResourceId> srcResourceIds, List<AzfsResourceId> destResourceIds)
            throws IOException {
        // TODO
    }

    @Override
    protected void rename(List<AzfsResourceId> srcResourceIds, List<AzfsResourceId> destResourceIds)
            throws IOException {
        // TODO
    }

    @Override
    protected void delete(Collection<AzfsResourceId> resourceIds) throws IOException{
        // TODO
    }

    @Override
    protected AzfsResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
        if (isDirectory) {
            if (!singleResourceSpec.endsWith("/")) {
                singleResourceSpec += "/";
            }
        } else {
            checkArgument(
                    !singleResourceSpec.endsWith("/"),
                    "Expected a file path, but [%s] ends with '/'. This is unsupported in AzfsFileSystem.",
                    singleResourceSpec);
        }
        return AzfsResourceId.fromUri(singleResourceSpec);
    }

}
