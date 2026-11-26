// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.backup;

import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BackupSnapshotTimeoutTest {
    @Test
    void largeTimeoutDoesNotExpireSnapshot() {
        // No table or file content is needed: getSnapshot creates file references only.
        // Adding a positive creation time to Long.MAX_VALUE used to produce an expired deadline.
        BackupJob job = new BackupJob();
        Deencapsulation.setField(job, "state", BackupJob.BackupJobState.FINISHED);
        Deencapsulation.setField(job, "repoId", Repository.KEEP_ON_LOCAL_REPO_ID);
        Deencapsulation.setField(job, "createTime", 1L);
        Deencapsulation.setField(job, "timeoutMs", Long.MAX_VALUE);
        Deencapsulation.setField(job, "localMetaInfoFilePath", "snapshot-meta");
        Deencapsulation.setField(job, "localJobInfoFilePath", "snapshot-job");
        Snapshot snapshot = job.getSnapshot();
        Assertions.assertNotNull(snapshot);
        Assertions.assertEquals(Long.MAX_VALUE, snapshot.getExpiredAt());
        Assertions.assertFalse(snapshot.isExpired());
        // A genuinely expired snapshot must keep its ordinary finite deadline.
        Deencapsulation.setField(job, "timeoutMs", 1L);
        snapshot = job.getSnapshot();
        Assertions.assertEquals(2L, snapshot.getExpiredAt());
        Assertions.assertTrue(snapshot.isExpired());
    }
}
