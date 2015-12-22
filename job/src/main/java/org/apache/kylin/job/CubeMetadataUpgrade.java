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

package org.apache.kylin.job;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeDescUpgrader;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.v1.CubeInstance;
import org.apache.kylin.cube.model.v1.CubeSegment;
import org.apache.kylin.cube.model.v1.CubeSegmentStatusEnum;
import org.apache.kylin.cube.model.v1.CubeStatusEnum;
import org.apache.kylin.job.common.HadoopShellExecutable;
import org.apache.kylin.job.common.MapReduceExecutable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.cube.CubingJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.hadoop.cube.BaseCuboidJob;
import org.apache.kylin.job.hadoop.cube.CubeHFileJob;
import org.apache.kylin.job.hadoop.cube.FactDistinctColumnsJob;
import org.apache.kylin.job.hadoop.cube.MergeCuboidJob;
import org.apache.kylin.job.hadoop.cube.NDCuboidJob;
import org.apache.kylin.job.hadoop.cube.RangeKeyDistributionJob;
import org.apache.kylin.job.hadoop.dict.CreateDictionaryJob;
import org.apache.kylin.job.hadoop.hbase.BulkLoadJob;
import org.apache.kylin.job.hadoop.hbase.CreateHTableJob;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This is the utility class to migrate the Kylin metadata format from v1 to v2;
 *
 * @author shaoshi
 */
public class CubeMetadataUpgrade {

    private KylinConfig config = null;
    private ResourceStore store;

    private List<String> updatedResources = Lists.newArrayList();
    private List<String> errorMsgs = Lists.newArrayList();

    private static final Log logger = LogFactory.getLog(CubeMetadataUpgrade.class);

    public CubeMetadataUpgrade(String newMetadataUrl) {
        KylinConfig.destoryInstance();
        System.setProperty(KylinConfig.KYLIN_CONF, newMetadataUrl);
        KylinConfig.getInstanceFromEnv().setMetadataUrl(newMetadataUrl);

        config = KylinConfig.getInstanceFromEnv();
        store = getStore();
    }

    public void upgrade() {

        upgradeTableDesc();
        upgradeTableDesceExd();
        upgradeCubeDesc();
        upgradeProjectInstance();
        upgradeCubeInstance();
        upgradeJobInstance();
        copyDictionaryForFK();
        verify();

    }

    public void cleanup() {
        MetadataManager.getInstance(config).reload();
        CubeDescManager.getInstance(config);
        CubeManager cubeManager = CubeManager.getInstance(config);

        List<String> activeResourceList = Lists.newArrayList();
        for (org.apache.kylin.cube.CubeInstance cube : cubeManager.listAllCubes()) {
            for (org.apache.kylin.cube.CubeSegment segment : cube.getSegments()) {
                activeResourceList.addAll(segment.getSnapshotPaths());
                activeResourceList.addAll(segment.getDictionaryPaths());
            }
        }

        List<String> toDeleteResource = Lists.newArrayList();
        List<String> activeResource = Lists.newArrayList();
        try {
            ArrayList<String> snapshotTables = getStore().listResources(ResourceStore.SNAPSHOT_RESOURCE_ROOT);

            for (String snapshotTable : snapshotTables) {
                ArrayList<String> snapshotNames = getStore().listResources(snapshotTable);
                if (snapshotNames != null)
                    for (String snapshot : snapshotNames) {
                        if (!activeResourceList.contains(snapshot)) {
                            toDeleteResource.add(snapshot);

                        } else {
                            activeResource.add(snapshot);
                        }
                    }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ArrayList<String> dictTables = getStore().listResources(ResourceStore.DICT_RESOURCE_ROOT);

            for (String table : dictTables) {
                ArrayList<String> tableColNames = getStore().listResources(table);
                if (tableColNames != null)
                    for (String tableCol : tableColNames) {
                        ArrayList<String> dictionaries = getStore().listResources(tableCol);
                        if (dictionaries != null)
                            for (String dict : dictionaries)
                                if (!activeResourceList.contains(dict)) {
                                    toDeleteResource.add(dict);
                                } else {
                                    activeResource.add(dict);
                                }
                    }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (toDeleteResource.size() > 0) {
            logger.info("The following resources is never needed, will be dropped, number :" + toDeleteResource.size());

            for (String s : toDeleteResource) {
                logger.info(s);
                try {
                    getStore().deleteResource(s);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void verify() {
        MetadataManager.getInstance(config).reload();
        CubeDescManager.clearCache();
        CubeDescManager.getInstance(config);
        CubeManager.getInstance(config);
        ProjectManager.getInstance(config);
        //cleanup();

    }

    private List<String> listResourceStore(String pathRoot) {
        List<String> paths = null;
        try {
            paths = store.collectResourceRecursively(pathRoot, MetadataConstants.FILE_SURFIX);
        } catch (IOException e1) {
            e1.printStackTrace();
            errorMsgs.add("Get IOException when scan resource store at: " + ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        }

        return paths;
    }

    private void upgradeCubeDesc() {
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        List<String> paths = listResourceStore(ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        for (String path : paths) {

            try {
                CubeDescUpgrader upgrade = new CubeDescUpgrader(path);
                CubeDesc ndesc = upgrade.upgrade();
                ndesc.setSignature(ndesc.calculateSignature());

                getStore().putResource(ndesc.getModel().getResourcePath(), ndesc.getModel(), MetadataManager.MODELDESC_SERIALIZER);
                getStore().putResource(ndesc.getResourcePath(), ndesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                updatedResources.add(ndesc.getResourcePath());
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade CubeDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }

    }

    private void upgradeTableDesc() {
        List<String> paths = listResourceStore(ResourceStore.TABLE_RESOURCE_ROOT);
        for (String path : paths) {
            TableDesc t;
            try {
                t = store.getResource(path, TableDesc.class, MetadataManager.TABLE_SERIALIZER);
                t.init();

                // if it only has 1 "." in the path, delete the old resource if it exists
                if (path.substring(path.indexOf(".")).length() == MetadataConstants.FILE_SURFIX.length()) {
                    getStore().deleteResource(path);
                    // the new source will be new;
                    t.setLastModified(0);
                    getStore().putResource(t.getResourcePath(), t, MetadataManager.TABLE_SERIALIZER);
                    updatedResources.add(t.getResourcePath());
                }
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade TableDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }

        }

    }

    @SuppressWarnings("unchecked")
    private void upgradeTableDesceExd() {

        List<String> paths = listResourceStore(ResourceStore.TABLE_EXD_RESOURCE_ROOT);
        for (String path : paths) {
            Map<String, String> attrs = Maps.newHashMap();

            InputStream is = null;
            try {
                is = store.getResource(path).inputStream;
                if (is == null) {
                    continue;
                }
                try {
                    attrs.putAll(JsonUtil.readValue(is, HashMap.class));
                } finally {
                    if (is != null)
                        is.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade TableDescExd at '" + path + "' failed: " + e.getLocalizedMessage());
            }

            // parse table identity from file name
            String file = path;
            if (file.indexOf("/") > -1) {
                file = file.substring(file.lastIndexOf("/") + 1);
            }
            String tableIdentity = file.substring(0, file.length() - MetadataConstants.FILE_SURFIX.length()).toUpperCase();

            // for metadata upgrade, convert resource path to new pattern (<DB>.<TABLE>.json)
            if (tableIdentity.indexOf(".") < 0) {
                tableIdentity = appendDBName(tableIdentity);
                try {
                    getMetadataManager().saveTableExd(tableIdentity, attrs);
                    //delete old resoruce if it exists;
                    getStore().deleteResource(path);
                    updatedResources.add(path);
                } catch (IOException e) {
                    e.printStackTrace();
                    errorMsgs.add("Upgrade TableDescExd at '" + path + "' failed: " + e.getLocalizedMessage());
                }

            }

        }

    }

    public String appendDBName(String table) {

        if (table.indexOf(".") > 0)
            return table;

        Map<String, TableDesc> map = this.getMetadataManager().getAllTablesMap();

        int count = 0;
        String result = null;
        for (TableDesc t : map.values()) {
            if (t.getName().equalsIgnoreCase(table)) {
                result = t.getIdentity();
                count++;
            }
        }

        if (count == 1)
            return result;

        if (count > 1) {
            errorMsgs.add("There are more than 1 table named with '" + table + "' in different database; ");
        }

        if (count == 0) {
            errorMsgs.add("No table definition for '" + table + "'; any project, cube refers it should remove the reference;");
        }

        return null;
    }

    private void upgradeProjectInstance() {
        List<String> paths = listResourceStore(ResourceStore.PROJECT_RESOURCE_ROOT);
        for (String path : paths) {
            try {
                org.apache.kylin.cube.model.v1.ProjectInstance oldPrj = store.getResource(path, org.apache.kylin.cube.model.v1.ProjectInstance.class, new JsonSerializer<org.apache.kylin.cube.model.v1.ProjectInstance>(org.apache.kylin.cube.model.v1.ProjectInstance.class));

                ProjectInstance newPrj = new ProjectInstance();
                newPrj.setUuid(oldPrj.getUuid());
                newPrj.setName(oldPrj.getName());
                newPrj.setOwner(oldPrj.getOwner());
                newPrj.setDescription(oldPrj.getDescription());
                newPrj.setLastModified(oldPrj.getLastModified());
                newPrj.setCreateTimeUTC(RootPersistentEntity.parseTime(oldPrj.getCreateTime()));
                newPrj.setStatus(oldPrj.getStatus());
                List<RealizationEntry> realizationEntries = Lists.newArrayList();
                for (String cube : oldPrj.getCubes()) {
                    RealizationEntry entry = new RealizationEntry();
                    entry.setType(RealizationType.CUBE);
                    entry.setRealization(cube);
                    realizationEntries.add(entry);
                }
                newPrj.setRealizationEntries(realizationEntries);

                Set<String> tables = Sets.newHashSet();
                for (String table : oldPrj.getTables()) {
                    String tb = this.appendDBName(table);
                    if (tb != null)
                        tables.add(this.appendDBName(tb));
                }
                newPrj.setTables(tables);

                store.putResource(newPrj.getResourcePath(), newPrj, ProjectManager.PROJECT_SERIALIZER);
                updatedResources.add(path);
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade Project at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }

    }

    private void upgradeCubeInstance() {

        ResourceStore store = getStore();
        List<String> paths = listResourceStore(ResourceStore.CUBE_RESOURCE_ROOT);
        for (String path : paths) {

            CubeInstance cubeInstance = null;
            try {
                cubeInstance = store.getResource(path, CubeInstance.class, new JsonSerializer<CubeInstance>(CubeInstance.class));
                cubeInstance.setConfig(config);

                org.apache.kylin.cube.CubeInstance newInstance = new org.apache.kylin.cube.CubeInstance();
                newInstance.setName(cubeInstance.getName());
                newInstance.setDescName(cubeInstance.getDescName());
                newInstance.setOwner(cubeInstance.getOwner());
                newInstance.setUuid(cubeInstance.getUuid());
                newInstance.setVersion(cubeInstance.getVersion());
                newInstance.setCreateTimeUTC(RootPersistentEntity.parseTime(cubeInstance.getCreateTime()));
                newInstance.setLastModified(cubeInstance.getLastModified());

                //status
                if (cubeInstance.getStatus() == CubeStatusEnum.BUILDING) {
                    newInstance.setStatus(RealizationStatusEnum.BUILDING);
                } else if (cubeInstance.getStatus() == CubeStatusEnum.DESCBROKEN) {
                    newInstance.setStatus(RealizationStatusEnum.DESCBROKEN);
                } else if (cubeInstance.getStatus() == CubeStatusEnum.DISABLED) {
                    newInstance.setStatus(RealizationStatusEnum.DISABLED);
                } else if (cubeInstance.getStatus() == CubeStatusEnum.READY) {
                    newInstance.setStatus(RealizationStatusEnum.READY);
                }

                List<org.apache.kylin.cube.CubeSegment> newSegments = Lists.newArrayList();
                // segment
                for (CubeSegment segment : cubeInstance.getSegments()) {
                    org.apache.kylin.cube.CubeSegment newSeg = new org.apache.kylin.cube.CubeSegment();
                    newSegments.add(newSeg);

                    newSeg.setUuid(segment.getUuid());
                    newSeg.setName(segment.getName());
                    newSeg.setStorageLocationIdentifier(segment.getStorageLocationIdentifier());
                    newSeg.setDateRangeStart(segment.getDateRangeStart());
                    newSeg.setDateRangeEnd(segment.getDateRangeEnd());

                    if (segment.getStatus() == CubeSegmentStatusEnum.NEW) {
                        newSeg.setStatus(SegmentStatusEnum.NEW);
                    } else if (segment.getStatus() == CubeSegmentStatusEnum.READY) {
                        newSeg.setStatus(SegmentStatusEnum.READY);
                    } else if (segment.getStatus() == CubeSegmentStatusEnum.READY_PENDING) {
                        newSeg.setStatus(SegmentStatusEnum.READY_PENDING);
                    }

                    newSeg.setSizeKB(segment.getSizeKB());
                    newSeg.setInputRecords(segment.getSourceRecords());
                    newSeg.setInputRecordsSize(segment.getSourceRecordsSize());
                    newSeg.setLastBuildTime(segment.getLastBuildTime());
                    newSeg.setLastBuildJobID(segment.getLastBuildJobID());
                    newSeg.setCreateTimeUTC(RootPersistentEntity.parseTime(segment.getCreateTime()));
                    newSeg.setBinarySignature(segment.getBinarySignature());

                    ConcurrentHashMap<String, String> newDictionaries = new ConcurrentHashMap<String, String>();

                    for (Map.Entry<String, String> e : segment.getDictionaries().entrySet()) {
                        String key = e.getKey();
                        String[] tableCol = StringUtils.split(key, "/");
                        key = appendDBName(tableCol[0]) + "/" + tableCol[1];
                        newDictionaries.put(key, e.getValue());
                    }
                    newSeg.setDictionaries(newDictionaries);

                    ConcurrentHashMap<String, String> newSnapshots = new ConcurrentHashMap<String, String>();

                    for (Map.Entry<String, String> e : segment.getSnapshots().entrySet()) {
                        newSnapshots.put(appendDBName(e.getKey()), e.getValue());
                    }
                    newSeg.setSnapshots(newSnapshots);
                }

                newInstance.setSegments(newSegments);
                store.putResource(newInstance.getResourcePath(), newInstance, CubeManager.CUBE_SERIALIZER);
            } catch (Exception e) {
                logger.error("Error during load cube instance " + path, e);
            }
        }
    }

    private void copyDictionaryForFK() {
        CubeManager cubeManager = CubeManager.getInstance(config);
        List<org.apache.kylin.cube.CubeInstance> cubeInstances = cubeManager.listAllCubes();

        Set<String> changedCubes = Sets.newHashSet();
        for (org.apache.kylin.cube.CubeInstance newInstance : cubeInstances) {

            boolean updated = false;
            DataModelDesc dataModelDesc = null;
            try {
                String modelName = this.getCubeDescManager().getCubeDesc(newInstance.getDescName()).getModelName();
                dataModelDesc = this.getMetadataManager().getDataModelDesc(modelName);
                Map<String, String> pkToFK = Maps.newHashMap();
                for (LookupDesc lookupDesc : dataModelDesc.getLookups()) {
                    if (lookupDesc.getJoin() != null) {
                        JoinDesc join = lookupDesc.getJoin();
                        for (int i = 0; i < join.getForeignKey().length; i++) {
                            pkToFK.put(lookupDesc.getTable() + "/" + join.getPrimaryKey()[i], dataModelDesc.getFactTable() + "/" + join.getForeignKey()[i]);
                        }
                    }
                }

                List<Pair<String, String>> newDictionaries = Lists.newArrayList();

                // segment
                for (org.apache.kylin.cube.CubeSegment newSeg : newInstance.getSegments()) {

                    for (Map.Entry<String, String> e : newSeg.getDictionaries().entrySet()) {
                        String key = e.getKey();
                        if (pkToFK.containsKey(key) && !newSeg.getDictionaries().containsKey(pkToFK.get(key))) {
                            logger.debug("Duplicate dictionary for FK " + pkToFK.get(key) + " in cube " + newInstance.getName());
                            changedCubes.add(newInstance.getName());
                            newDictionaries.add(new Pair<String, String>(pkToFK.get(key), e.getValue()));

                        }
                    }
                    for (Pair<String, String> dict : newDictionaries) {
                        newSeg.getDictionaries().put(dict.getFirst(), dict.getSecond());
                        updated = true;
                    }
                }

                if (updated)
                    store.putResource(newInstance.getResourcePath(), newInstance, CubeManager.CUBE_SERIALIZER);
            } catch (Exception e) {
                logger.error("Error during upgrade cube instance " + newInstance.getName(), e);
            }
        }

        if (changedCubes.size() > 0)
            logger.info("Updated these cubeInstances: " + changedCubes);
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    private ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(config);
    }

    private void upgradeJobInstance() {
        try {
            List<String> paths = getStore().collectResourceRecursively(ResourceStore.JOB_PATH_ROOT, "");
            for (String path : paths) {
                upgradeJobInstance(path);
            }

            for (String folder : new String[] { ResourceStore.JOB_PATH_ROOT, ResourceStore.JOB_OUTPUT_PATH_ROOT }) {
                for (String res : getStore().listResources(folder)) {
                    getStore().deleteResource(res);
                }
                getStore().deleteResource(folder);
            }
        } catch (IOException ex) {
            errorMsgs.add("upgrade job failed" + ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }

    }

    private ExecutableState parseState(JobStatusEnum state) {
        switch (state) {
        case NEW:
        case PENDING:
            return ExecutableState.READY;
        case RUNNING:
            return ExecutableState.RUNNING;
        case FINISHED:
            return ExecutableState.SUCCEED;
        case ERROR:
            return ExecutableState.ERROR;
        case DISCARDED:
            return ExecutableState.DISCARDED;
        default:
            return ExecutableState.DISCARDED;
        }
    }

    private ExecutableState parseState(JobStepStatusEnum state) {
        switch (state) {
        case NEW:
        case PENDING:
        case WAITING:
            return ExecutableState.READY;
        case RUNNING:
            return ExecutableState.RUNNING;
        case FINISHED:
            return ExecutableState.SUCCEED;
        case ERROR:
            return ExecutableState.ERROR;
        case DISCARDED:
            return ExecutableState.DISCARDED;
        default:
            return ExecutableState.DISCARDED;
        }

    }

    private void upgradeJobInstance(String path) throws IOException {
        JobInstance job = getStore().getResource(path, JobInstance.class, new JsonSerializer<JobInstance>(JobInstance.class));
        long lastModified = job.getLastModified();
        if (System.currentTimeMillis() - lastModified > 2592000000l) {
            // old than 30 days, skip;
            return;
        }
        CubingJob cubingJob = new CubingJob();
        cubingJob.setId(job.getId());
        cubingJob.setName(job.getName());
        cubingJob.setCubeName(job.getRelatedCube());
        cubingJob.setSubmitter(job.getSubmitter());
        for (JobInstance.JobStep step : job.getSteps()) {
            final AbstractExecutable executable = parseToExecutable(step);
            cubingJob.addTask(executable);
        }
        getExecutableManager().addJob(cubingJob);

        cubingJob.setStartTime(job.getExecStartTime());
        cubingJob.setEndTime(job.getExecEndTime());
        cubingJob.setMapReduceWaitTime(job.getMrWaiting());
        getExecutableManager().resetJobOutput(cubingJob.getId(), parseState(job.getStatus()), job.getStatus().toString());

        for (int i = 0, size = job.getSteps().size(); i < size; ++i) {
            final JobInstance.JobStep jobStep = job.getSteps().get(i);
            final String outputPath = ResourceStore.JOB_OUTPUT_PATH_ROOT + "/" + job.getId() + "." + i;
            final InputStream inputStream = getStore().getResource(outputPath).inputStream;

            String output = null;
            if (inputStream != null) {
                @SuppressWarnings("unchecked")
                HashMap<String, String> job_output = JsonUtil.readValue(inputStream, HashMap.class);

                if (job_output != null) {
                    output = job_output.get("output");
                }
                org.apache.commons.io.IOUtils.closeQuietly(inputStream);
            }
            updateJobStepOutput(jobStep, output, cubingJob.getTasks().get(i));
        }
    }

    private void updateJobStepOutput(JobInstance.JobStep step, String output, AbstractExecutable task) {
        task.setStartTime(step.getExecStartTime());
        task.setEndTime(step.getExecEndTime());
        if (task instanceof MapReduceExecutable) {
            ((MapReduceExecutable) task).setMapReduceWaitTime(step.getExecWaitTime() * 1000);
        }
        getExecutableManager().resetJobOutput(task.getId(), parseState(step.getStatus()), output);
    }

    private AbstractExecutable parseToExecutable(JobInstance.JobStep step) {
        AbstractExecutable result;
        switch (step.getCmdType()) {
        case SHELL_CMD_HADOOP: {
            ShellExecutable executable = new ShellExecutable();
            executable.setCmd(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_FACTDISTINCT: {
            MapReduceExecutable executable = new MapReduceExecutable();
            executable.setMapReduceJobClass(FactDistinctColumnsJob.class);
            executable.setMapReduceParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_BASECUBOID: {
            MapReduceExecutable executable = new MapReduceExecutable();
            executable.setMapReduceJobClass(BaseCuboidJob.class);
            executable.setMapReduceParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_NDCUBOID: {
            MapReduceExecutable executable = new MapReduceExecutable();
            executable.setMapReduceJobClass(NDCuboidJob.class);
            executable.setMapReduceParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION: {
            MapReduceExecutable executable = new MapReduceExecutable();
            executable.setMapReduceJobClass(RangeKeyDistributionJob.class);
            executable.setMapReduceParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_CONVERTHFILE: {
            MapReduceExecutable executable = new MapReduceExecutable();
            executable.setMapReduceJobClass(CubeHFileJob.class);
            executable.setMapReduceParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_MERGECUBOID: {
            MapReduceExecutable executable = new MapReduceExecutable();
            executable.setMapReduceJobClass(MergeCuboidJob.class);
            executable.setMapReduceParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_NO_MR_DICTIONARY: {
            HadoopShellExecutable executable = new HadoopShellExecutable();
            executable.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
            executable.setJobClass(CreateDictionaryJob.class);
            executable.setJobParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE: {
            HadoopShellExecutable executable = new HadoopShellExecutable();
            executable.setJobClass(CreateHTableJob.class);
            executable.setJobParams(step.getExecCmd());
            result = executable;
            break;
        }
        case JAVA_CMD_HADOOP_NO_MR_BULKLOAD: {
            HadoopShellExecutable executable = new HadoopShellExecutable();
            executable.setJobClass(BulkLoadJob.class);
            executable.setJobParams(step.getExecCmd());
            result = executable;
            break;
        }
        default:
            throw new RuntimeException("invalid step type:" + step.getCmdType());
        }
        result.setName(step.getName());
        return result;
    }

    public static void main(String[] args) {

        if (!(args != null && (args.length == 1 || args.length == 2))) {
            System.out.println("Usage: java CubeMetadataUpgrade <metadata_export_folder> <verify>; e.g, /export/kylin/meta ");
            return;
        }

        String exportFolder = args[0];
        boolean verify = false;
        if (args.length == 2 && "verify".equals(args[1])) {
            System.out.println("Only verify the metadata in folder " + exportFolder);
            verify = true;
        }

        CubeMetadataUpgrade instance = null;
        if (verify) {
            instance = new CubeMetadataUpgrade(exportFolder);
            instance.verify();
            instance.copyDictionaryForFK();
        } else {
            File oldMetaFolder = new File(exportFolder);
            if (!oldMetaFolder.exists()) {
                System.out.println("Provided folder doesn't exist: '" + exportFolder + "'");
                return;
            }

            if (!oldMetaFolder.isDirectory()) {
                System.out.println("Provided folder is not a directory: '" + exportFolder + "'");
                return;
            }

            String newMetadataUrl = oldMetaFolder.getAbsolutePath() + "_v2";
            try {
                FileUtils.deleteDirectory(new File(newMetadataUrl));
                FileUtils.copyDirectory(oldMetaFolder, new File(newMetadataUrl));
            } catch (IOException e) {
                e.printStackTrace();
            }

            instance = new CubeMetadataUpgrade(newMetadataUrl);
            instance.upgrade();
            logger.info("=================================================================");
            logger.info("Run CubeMetadataUpgrade completed;");

        }

        logger.info("=================================================================");
        if (instance.errorMsgs.size() > 0) {
            logger.info("Here are the error/warning messages, you may need check:");
            for (String s : instance.errorMsgs) {
                logger.warn(s);
            }
        } else {
            logger.info("No error or warning messages; The migration is success.");
        }
    }
}
