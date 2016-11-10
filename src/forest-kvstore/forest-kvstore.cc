/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "forest-kvstore/forest-kvstore.h"

#include "common.h"

#include <sys/stat.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <vbucket.h>
#include <JSON_checker.h>
#include <locks.h>

#define STATWRITER_NAMESPACE forestdb_engine
#include "statwriter.h"
#undef STATWRITER_NAMESPACE

using namespace CouchbaseDirectoryUtilities;

std::mutex ForestKVStore::initLock;
int ForestKVStore::numGlobalFiles(0);

extern "C" {
    static fdb_compact_decision compaction_cb_c(fdb_file_handle* fhandle,
                            fdb_compaction_status status, const char* kv_name,
                            fdb_doc* doc, uint64_t old_offset,
                            uint64_t new_offset, void* ctx) {
        return ForestKVStore::compaction_cb(fhandle, status, kv_name, doc,
                                            old_offset, new_offset, ctx);
    }

    static void errorlog_cb(int err_code, const char* err_msg, void* ctx_data) {
        LOG(EXTENSION_LOG_WARNING, "%s with error: %s",
                                   err_msg,
                                   fdb_error_msg(static_cast<fdb_status>(err_code)));
    }

    struct fdb_stats_cb_ctx {
        std::map<std::string, uint64_t> stats;
    };

    void fdbStatsCallback(fdb_kvs_handle* handle, const char* stat,
                          uint64_t value, void *ctx) {
        fdb_stats_cb_ctx *statsCtx = static_cast<fdb_stats_cb_ctx*>(ctx);
        statsCtx->stats[stat] = value;
    }
}


void ForestKVStore::initForestDb() {
    LockHolder lh(initLock);
    if (numGlobalFiles == 0) {
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        fdb_status status = fdb_init(&fileConfig);
        if (status != FDB_RESULT_SUCCESS) {
            throw std::logic_error("ForestKVStore::initForestDb: failed "
                                   "with status:" + std::to_string(status));
        }
        ObjectRegistry::onSwitchThread(epe);
    }
    ++numGlobalFiles;
}

void ForestKVStore::shutdownForestDb() {
   LockHolder lh(initLock);
   if (--numGlobalFiles == 0) {
       EventuallyPersistentEngine* epe = ObjectRegistry::onSwitchThread(NULL, true);
       fdb_status status = fdb_shutdown();
       if (status != FDB_RESULT_SUCCESS) {
           logger.log(EXTENSION_LOG_WARNING, "Shutting down forestdb failed "
                                             "with error: %s",
                                             fdb_error_msg(status));
       }
       ObjectRegistry::onSwitchThread(epe);
   }
}

ForestKVStore::ForestKVStore(KVStoreConfig &config, bool read_only)
    : KVStore(config, read_only),
      dbname(config.getDBName()),
      scanCounter(0)
{
    /* create the data directory */
    createDataDir(dbname);

    statCollectingFileOps = getForestStatOps(&st.fsStats);

    fileConfig = fdb_get_default_config();
    kvsConfig = fdb_get_default_kvs_config();

    /* Set the purge interval to the maximum possible value to ensure
     * that deleted items don't get removed immediately. The tombstone
     * items will be purged in the expiry callback.
     */
    fileConfig.purging_interval = std::numeric_limits<uint32_t>::max();
    /* Since DCP requires sequence based iteration, enable sequence tree
     * indexes in forestdb
     */
    fileConfig.seqtree_opt = FDB_SEQTREE_USE;
    /* Enable compression of document body in order to occupy less
     * space on disk
     */
    fileConfig.compress_document_body = true;
    /* Disable breakpad. The memcached binary already has breakpad
     * initialized. ForestDB is already part of the memcached
     * binary and thus doesn't have to be initialized again
     * for ForestDB.
     */
    fileConfig.breakpad_minidump_dir = nullptr;

    fileConfig.custom_file_ops = &statCollectingFileOps;

    /* Set the buffer cache value to 6 GiB for performance */
    fileConfig.buffercache_size = 6442450944;

    /* Setting WAL threshold to 4K */
    fileConfig.wal_threshold = 4096;

    /* Disable block reuse */
    fileConfig.block_reusing_threshold = 100;

    /* Enabling auto commit */
    fileConfig.auto_commit = true;

    // init db file map with default revision number, 1
    numDbFiles = configuration.getMaxVBuckets();
    cachedVBStates.reserve(numDbFiles);

    // pre-allocate lookup maps (vectors) given we have a relatively
    // small, fixed number of vBuckets.
    dbFileRevMap.assign(numDbFiles, Couchbase::RelaxedAtomic<uint64_t>(1));
    cachedDocCount.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(0));
    cachedDeleteCount.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(-1));
    cachedFileSize.assign(numDbFiles, Couchbase::RelaxedAtomic<uint64_t>(0));
    cachedSpaceUsed.assign(numDbFiles, Couchbase::RelaxedAtomic<uint64_t>(0));
    cachedVBStates.assign(numDbFiles, nullptr);

    // FDB/KV handle pools
    rwFKvsHandleMap.assign(numDbFiles, nullptr);
    roFKvsHandleMap.assign(numDbFiles, nullptr);
    stateFKvsHandleMap.assign(numDbFiles, nullptr);

    // Initialize handle list used during compaction callback(s)
    compactCBKvsHandles.assign(numDbFiles, nullptr);

    // pre-allocate all files' block cache hits and block cache misses
    cachedBlockCacheHits.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(0));
    cachedBlockCacheMisses.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(0));
    cachedBlockCacheNumItems.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(0));
    cachedBlockCacheNumVictims.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(0));
    cachedBlockCacheNumImmutables.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(0));

    initialize();
}

ForestKVStore::ForestKVStore(const ForestKVStore &copyFrom)
    : KVStore(copyFrom),
      dbname(copyFrom.dbname)
{
    /* create the data directory */
    createDataDir(dbname);
    statCollectingFileOps = getForestStatOps(&st.fsStats);
}

static void discoverDbFiles(const std::string& dir,
                            std::vector<std::string> &v) {
    std::vector<std::string> files = findFilesContaining(dir, ".fdb");
    for (auto& it : files) {
        v.push_back(it);
    }
}

static std::string getDBFileName(const std::string &dbname,
                                 uint16_t vbid,
                                 uint64_t rev) {
    std::stringstream ss;
    ss << dbname << "/" << vbid << ".fdb." << rev;
    return ss.str();
}

void ForestKVStore::initialize() {
    std::vector<uint16_t> vbids;
    std::vector<std::string> files;
    discoverDbFiles(dbname, files);
    populateFileNameMap(dbname, files, vbids, "fdb");

    initForestDb();

    for (auto& itr : vbids) {
        readVBState(itr);
        ++st.numLoadedVb;
    }
}

fdb_config ForestKVStore::getFileConfig() {
    return fileConfig;
}

fdb_kvs_config ForestKVStore::getKVConfig() {
    return kvsConfig;
}

ForestKVStore::~ForestKVStore() {
    close();

    for (auto& it : cachedVBStates) {
        vbucket_state* vbstate = it;
        if (vbstate) {
            delete vbstate;
        }
    }

    for (size_t i = 0; i < stateFKvsHandleMap.size() +
                           roFKvsHandleMap.size() +
                           rwFKvsHandleMap.size(); ++i) {
        st.numClose++;
    }
    stateFKvsHandleMap.clear();
    roFKvsHandleMap.clear();
    rwFKvsHandleMap.clear();

    shutdownForestDb();
}

void ForestKVStore::reset(uint16_t vbId) {
    {
        std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                      vbId, HandleType::WRITER);
        // Cancel any on-going compaction on the file
        fdb_cancel_compaction(fkvsHandle->getFileHandle());
    }

    rwFKvsHandleMap[vbId] = nullptr;
    roFKvsHandleMap[vbId] = nullptr;
    stateFKvsHandleMap[vbId] = nullptr;

    st.numClose += 3;

    std::string dbFile = getDBFileName(dbname, vbId, dbFileRevMap[vbId]);
    fdb_status status = FDB_RESULT_SUCCESS;
    status = fdb_destroy(dbFile.c_str(), &fileConfig);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "ForestKVStore::reset: fdb_destroy failed for vb:%" PRIu16 ", "
            "with error: %s",
            vbId, fdb_error_msg(status));

        /** Requeue file deletion if the return status is anything but
         * FDB_RESULT_NO_SUCH_FILE */
        if (status != FDB_RESULT_NO_SUCH_FILE) {

            LOG(EXTENSION_LOG_WARNING,
                "ForestKVStore::reset: Queueing file deletion for vb:%" PRIu16,
                vbId);

            pendingFileDeletions.push(dbFile);
        }
    }

    vbucket_state* state = cachedVBStates[vbId];
    if (!state) {
        throw std::invalid_argument("ForestKVStore::reset::No entry "
                                    "in cached states for vbucket " +
                                    std::to_string(vbId));
    }

    state->reset();

    setVBucketState(vbId, state);

    cachedDocCount[vbId] = 0;
    cachedDeleteCount[vbId] = 0;
    cachedFileSize[vbId] = 0;
    cachedSpaceUsed[vbId] = 0;
    cachedBlockCacheHits[vbId] = 0;
    cachedBlockCacheMisses[vbId] = 0;
    cachedBlockCacheNumItems[vbId] = 0;
    cachedBlockCacheNumVictims[vbId] = 0;
    cachedBlockCacheNumImmutables[vbId] = 0;

    updateDbFileMap(vbId, 1);
}

ForestRequest::ForestRequest(const Item &it, MutationRequestCallback &cb,
                             bool del, size_t itmDataSize = 0)
    : IORequest(it.getVBucketId(), cb, del, it.getKey()),
      status(MUTATION_SUCCESS),
      dataSize(itmDataSize) { }

ForestRequest::~ForestRequest() { }

void ForestKVStore::close() {
    intransaction = false;
}

ENGINE_ERROR_CODE ForestKVStore::readVBState(uint16_t vbId) {
    fdb_status status = FDB_RESULT_SUCCESS;
    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    std::string failovers;
    int64_t highSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;

    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                 vbId, HandleType::STATE_SNAP);

    fdb_kvs_info kvsInfo;
    status = fdb_get_kvs_info(fkvsHandle->getKvsHandle(), &kvsInfo);
    //TODO: Update the purge sequence number
    if (status == FDB_RESULT_SUCCESS) {
        highSeqno = static_cast<int64_t>(kvsInfo.last_seqnum);
    } else {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::readVBState: Failed to "
            "read KV Store info for vbucket: %d with error: %s", vbId,
            fdb_error_msg(status));
        return forestErr2EngineErr(status);
    }

    char keybuf[20];
    int len = snprintf(keybuf, sizeof(keybuf), "vb%" PRIu16, vbId);
    if (len < 0) {
        throw std::runtime_error("ForestKVStore::readVBState: Formatting error "
                                 "in generating the KVStore name for vb:" +
                                 std::to_string(vbId));
    }

    fdb_doc* statDoc;
    fdb_doc_create(&statDoc, (void *)keybuf, strlen(keybuf), NULL, 0, NULL, 0);

    status = fdb_get(fkvsHandle->getKvsHandle(), statDoc);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_DEBUG,
            "ForestKVStore::readVBState: Failed to retrieve vbucket state for "
            "vBucket=%d with error=%s", vbId , fdb_error_msg(status));
    } else {
        cJSON* jsonObj = cJSON_Parse((char *)statDoc->body);

        if (!jsonObj) {
            fdb_doc_free(statDoc);
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::readVBState: Failed to "
                "parse the vbstat json doc for vbucket: %d: %s", vbId,
                (char *)statDoc->body);
            return forestErr2EngineErr(status);
        }

        const std::string vb_state = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "state"));

        const std::string checkpoint_id = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj,"checkpoint_id"));

        const std::string max_deleted_seqno = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "max_deleted_seqno"));

        const std::string snapStart = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "snap_start"));

        const std::string snapEnd = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "snap_end"));

        const std::string maxCasValue = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "max_cas"));

        cJSON* failover_json = cJSON_GetObjectItem(jsonObj, "failover_table");
        if (vb_state.compare("") == 0 || checkpoint_id.compare("") == 0
               || max_deleted_seqno.compare("") == 0) {
             LOG(EXTENSION_LOG_WARNING,
                 "ForestKVStore::readVBState: State JSON "
                 "doc for vbucket: %d is in the wrong format: %s,"
                 "vb state: %s, checkpoint id: %s and max deleted seqno: %s",
                 vbId, (char *)statDoc->body, vb_state.c_str(),
                 checkpoint_id.c_str(), max_deleted_seqno.c_str());
        } else {
            state = VBucket::fromString(vb_state.c_str());
            parseUint64(max_deleted_seqno.c_str(), &maxDeletedSeqno);
            parseUint64(checkpoint_id.c_str(), &checkpointId);

            if (snapStart.compare("")) {
                parseUint64(snapStart.c_str(), &lastSnapStart);
            }

            if (snapEnd.compare("")) {
                parseUint64(snapEnd.c_str(), &lastSnapEnd);
            }

            if (maxCasValue.compare("")) {
                parseUint64(maxCasValue.c_str(), &maxCas);
            }

            if (failover_json) {
                char* json = cJSON_PrintUnformatted(failover_json);
                failovers.assign(json);
                cJSON_Free(json);
            }
        }

        cJSON_Delete(jsonObj);
    }

    if (failovers.empty()) {
        failovers.assign("[{\"id\":0,\"seq\":0}]");
    }

    delete cachedVBStates[vbId];
    cachedVBStates[vbId] = new vbucket_state(state, checkpointId,
                                             maxDeletedSeqno, highSeqno, 0,
                                             lastSnapStart, lastSnapEnd,
                                             maxCas, failovers);
    fdb_doc_free(statDoc);
    return forestErr2EngineErr(status);
}

bool ForestKVStore::setVBucketState(uint16_t vbId, vbucket_state* vbstate) {
    if (!vbstate) {
        throw std::invalid_argument("ForestKVStore::setVBucketState: No vb "
                                    "state provided for vbucket: " +
                                    std::to_string(vbId));
    }

    fdb_status status;
    std::string stateStr = vbstate->toJSON();

    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                        vbId,
                                                        HandleType::STATE_SNAP);

    if (!stateStr.empty()) {
        fdb_doc statDoc;
        memset(&statDoc, 0, sizeof(statDoc));
        char keybuf[20];
        int len = snprintf(keybuf, sizeof(keybuf), "vb%" PRIu16, vbId);
        if (len < 0) {
            throw std::runtime_error("ForestKVStore::setVBucketState: Formatting "
                                     "error in generating the KVStore name for "
                                     "vb:" + std::to_string(vbId));
        }
        statDoc.key = keybuf;
        statDoc.keylen = strlen(keybuf);
        statDoc.meta = NULL;
        statDoc.metalen = 0;
        statDoc.body = const_cast<char *>(stateStr.c_str());
        statDoc.bodylen = stateStr.length();

        status = fdb_set(fkvsHandle->getKvsHandle(), &statDoc);
        if (status != FDB_RESULT_SUCCESS) {
            logger.log(EXTENSION_LOG_WARNING,
                       "ForestKVStore::setVBucketState:Failed to save "
                       "vbucket state for vbucket=%" PRIu16" error=%s", vbId,
                       fdb_error_msg(status));
            return false;
        }
    }

    return true;
}

void ForestKVStore::updateFileInfo(ForestKvsHandle* fKvsHandle,
                                   uint16_t vbId) {
    if (!fKvsHandle) {
        return;
    }

    fdb_status status;

    fdb_file_info finfo;
    status = fdb_get_file_info(fKvsHandle->getFileHandle(), &finfo);
    if (status == FDB_RESULT_SUCCESS) {
        cachedFileSize[vbId] = finfo.file_size;
        cachedSpaceUsed[vbId] = finfo.space_used;
    } else {
        logger.log(EXTENSION_LOG_WARNING,
                "ForestKVStore::updateFileInfo: Getting file info"
                " failed with error: %s for vbucket id: %" PRIu16,
                fdb_error_msg(status), vbId);
    }

    fdb_kvs_info kvsInfo;
    status = fdb_get_kvs_info(fKvsHandle->getKvsHandle(), &kvsInfo);
    if (status == FDB_RESULT_SUCCESS) {
        cachedDocCount[vbId] = kvsInfo.doc_count;
        cachedDeleteCount[vbId] = kvsInfo.deleted_count;
    } else {
        logger.log(EXTENSION_LOG_WARNING,
                   "ForestKVStore::updateFileInfo: Getting kvs information "
                   "failed with error: %s for vbucket id: %" PRIu16,
                   fdb_error_msg(status), vbId);
    }
}

bool ForestKVStore::delVBucket(uint16_t vbId) {
    {
        std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                      vbId, HandleType::WRITER);
        // Cancel any on-going compaction on the file
        fdb_cancel_compaction(fkvsHandle->getFileHandle());
    }

    rwFKvsHandleMap[vbId] = nullptr;
    roFKvsHandleMap[vbId] = nullptr;
    stateFKvsHandleMap[vbId] = nullptr;

    st.numClose += 3;

    fdb_status status = fdb_destroy(getDBFileName(dbname,
                                                  vbId,
                                                  dbFileRevMap[vbId]).c_str(),
                                    &fileConfig);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "ForestKVStore::delVBucket: fdb_destroy failed for vbucket "
            "%" PRIu16 "with error: %s", vbId, fdb_error_msg(status));

        /** Requeue delVBucket task if the return status is anything but
         * FDB_RESULT_NO_SUCH_FILE */
        if (status != FDB_RESULT_NO_SUCH_FILE) {

            LOG(EXTENSION_LOG_WARNING,
                "ForestKVStore::delVBucket: rescheduling vbucket deletion for "
                "vbucket %" PRIu16, vbId);

            return false;
        }
    }

    if (cachedVBStates[vbId]) {
        delete cachedVBStates[vbId];
    }

    std::string failovers("[{\"id\":0, \"seq\":0}]");
    cachedVBStates[vbId] = new vbucket_state(vbucket_state_dead, 0, 0, 0, 0,
                                             0, 0, 0, failovers);

    cachedDocCount[vbId] = 0;
    cachedDeleteCount[vbId] = 0;
    cachedFileSize[vbId] = 0;
    cachedSpaceUsed[vbId] = 0;
    cachedBlockCacheHits[vbId] = 0;
    cachedBlockCacheMisses[vbId] = 0;
    cachedBlockCacheNumItems[vbId] = 0;
    cachedBlockCacheNumVictims[vbId] = 0;
    cachedBlockCacheNumImmutables[vbId] = 0;

    updateDbFileMap(vbId, 1);

    return true;
}

ENGINE_ERROR_CODE ForestKVStore::forestErr2EngineErr(fdb_status errCode) {
    switch (errCode) {
    case FDB_RESULT_SUCCESS:
        return ENGINE_SUCCESS;
    case FDB_RESULT_ALLOC_FAIL:
        return ENGINE_ENOMEM;
    case FDB_RESULT_KEY_NOT_FOUND:
        return ENGINE_KEY_ENOENT;
    case FDB_RESULT_NO_SUCH_FILE:
    case FDB_RESULT_NO_DB_HEADERS:
    default:
        // same as the general error return code of
        // EPBucket::getInternal
        return ENGINE_TMPFAIL;
    }
}

void ForestKVStore::getWithHeader(void* handle, const std::string& key,
                                  uint16_t vb, Callback<GetValue>& cb,
                                  bool fetchDelete) {
    fdb_kvs_handle* kvsHandle = static_cast<fdb_kvs_handle*>(handle);
    if (!kvsHandle) {
        throw std::invalid_argument("ForestKVStore::getWithHeader: "
                                    "KVS Handle is NULL for vbucket id:" +
                                    std::to_string(vb));
    }

    hrtime_t start = gethrtime();
    RememberingCallback<GetValue> *rc =
                       dynamic_cast<RememberingCallback<GetValue> *>(&cb);
    bool getMetaOnly = rc && rc->val.isPartial();
    GetValue rv;
    fdb_status status;

    fdb_doc rdoc;
    memset(&rdoc, 0, sizeof(rdoc));
    rdoc.key = const_cast<char *>(key.c_str());
    rdoc.keylen = key.length();

    if (!getMetaOnly) {
        status = fdb_get(kvsHandle, &rdoc);
    } else {
        status = fdb_get_metaonly(kvsHandle, &rdoc);
    }

    if (status != FDB_RESULT_SUCCESS) {
        if (!getMetaOnly) {
            LOG(EXTENSION_LOG_WARNING,
                "ForestKVStore::getWithHeader: Failed to retrieve metadata from "
                "database, vbucketId:%d key:%s error:%s\n",
                vb, key.c_str(), fdb_error_msg(status));
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "ForestKVStore::getWithHeader: Failed to retrieve key value from database,"
                "vbucketId:%d key:%s error:%s deleted:%s", vb, key.c_str(),
                fdb_error_msg(status), rdoc.deleted ? "yes" : "no");
        }
        ++st.numGetFailure;
    } else {
        rv = docToItem(kvsHandle, &rdoc, vb, getMetaOnly, fetchDelete);
    }

    rdoc.key = NULL;
    fdb_free_block(rdoc.meta);
    fdb_free_block(rdoc.body);

    st.readTimeHisto.add((gethrtime() - start) / 1000);
    if (status == FDB_RESULT_SUCCESS) {
        st.readSizeHisto.add(key.length() + rv.getValue()->getNBytes());
    }

    rv.setStatus(forestErr2EngineErr(status));
    cb.callback(rv);
}

static ForestMetaData forestMetaDecode(const fdb_doc *rdoc) {
    char* metadata = static_cast<char *>(rdoc->meta);
    ForestMetaData forestMetaData;

    memcpy(&forestMetaData.cas, metadata + forestMetaOffset(cas),
           sizeof(forestMetaData.cas));
    memcpy(&forestMetaData.rev_seqno, metadata + forestMetaOffset(rev_seqno),
           sizeof(forestMetaData.rev_seqno));
    memcpy(&forestMetaData.exptime, metadata + forestMetaOffset(exptime),
           sizeof(forestMetaData.exptime));
    memcpy(&forestMetaData.texptime, metadata + forestMetaOffset(texptime),
           sizeof(forestMetaData.texptime));
    memcpy(&forestMetaData.flags, metadata + forestMetaOffset(flags),
           sizeof(forestMetaData.flags));
    memcpy(&forestMetaData.ext_meta, metadata + forestMetaOffset(ext_meta),
           EXT_META_LEN);

    forestMetaData.cas = ntohll(forestMetaData.cas);
    forestMetaData.rev_seqno = ntohll(forestMetaData.rev_seqno);
    forestMetaData.exptime = ntohl(forestMetaData.exptime);
    forestMetaData.texptime = ntohl(forestMetaData.texptime);

    return forestMetaData;
}

fdb_changes_decision ForestKVStore::recordChanges(fdb_kvs_handle* handle,
                                                  fdb_doc* doc,
                                                  void* ctx) {

    ScanContext* sctx = static_cast<ScanContext*>(ctx);

    const uint64_t byseqno = doc->seqnum;
    const uint16_t vbucketId = sctx->vbid;

    std::string docKey((char*)doc->key, doc->keylen);
    CacheLookup lookup(docKey, byseqno, vbucketId);

    std::shared_ptr<Callback<CacheLookup> > lookup_cb = sctx->lookup;
    lookup_cb->callback(lookup);

    switch (lookup_cb->getStatus()) {
        case ENGINE_SUCCESS:
            /* Go ahead and create an entry */
            break;
        case ENGINE_KEY_EEXISTS:
            /* Key already exists */
            sctx->lastReadSeqno = byseqno;
            return FDB_CHANGES_CLEAN;
        case ENGINE_ENOMEM:
            /* High memory pressure, cancel execution */
            sctx->logger->log(EXTENSION_LOG_WARNING,
                              "ForestKVStore::recordChanges: "
                              "Out of memory, vbucket: %" PRIu16
                              ", cancelling the iteration!", vbucketId);
            return FDB_CHANGES_CANCEL;
        default:
            std::string err("ForestKVStore::recordChanges: Invalid response: "
                            + std::to_string(lookup_cb->getStatus()));
            throw std::logic_error(err);
    }

    ForestMetaData meta = forestMetaDecode(doc);

    void* valuePtr = nullptr;
    size_t valueLen = 0;

    if (sctx->valFilter != ValueFilter::KEYS_ONLY && !doc->deleted) {
        valuePtr = doc->body;
        valueLen = doc->bodylen;
    }

    Item* it = new Item(doc->key, doc->keylen,
                        meta.flags,
                        meta.exptime,
                        valuePtr, valueLen,
                        meta.ext_meta, EXT_META_LEN,
                        meta.cas,
                        byseqno,
                        vbucketId,
                        meta.rev_seqno);
    if (doc->deleted) {
        it->setDeleted();
    }

    bool onlyKeys = (sctx->valFilter == ValueFilter::KEYS_ONLY) ? true : false;
    GetValue rv(it, ENGINE_SUCCESS, -1, onlyKeys);

    std::shared_ptr<Callback<GetValue> > getval_cb = sctx->callback;
    getval_cb->callback(rv);

    switch (getval_cb->getStatus()) {
        case ENGINE_SUCCESS:
            /* Success */
            break;
        case ENGINE_KEY_ENOENT:
            /* If in case of rollback's CB, if an item to delete
               isn't in the in-memory hash table, then an ENOENT
               is returned, is benign. */
            break;
        case ENGINE_ENOMEM:
            /* High memory pressure, cancel execution */
            sctx->logger->log(EXTENSION_LOG_WARNING,
                              "ForestKVStore::recordChanges: "
                              "Out of memory, vbucket: %" PRIu16
                              ", cancelling iteration!", vbucketId);
            return FDB_CHANGES_CANCEL;
        default:
            std::string err("ForestKVStore::recordChanges: "
                            "Unexpected error code: " +
                            std::to_string(getval_cb->getStatus()));
            throw std::logic_error(err);
    }

    sctx->lastReadSeqno = byseqno;
    return FDB_CHANGES_CLEAN;
}

GetValue ForestKVStore::docToItem(fdb_kvs_handle* kvsHandle, fdb_doc* rdoc,
                                  uint16_t vbId, bool metaOnly, bool fetchDelete) {
    ForestMetaData forestMetaData;

    //TODO: handle metadata upgrade?
    forestMetaData = forestMetaDecode(rdoc);

    Item* it = NULL;

    if (metaOnly || (fetchDelete && rdoc->deleted)) {
        it = new Item((char *)rdoc->key, rdoc->keylen, forestMetaData.flags,
                      forestMetaData.exptime, NULL, 0, forestMetaData.ext_meta,
                      EXT_META_LEN, forestMetaData.cas,
                      (uint64_t)rdoc->seqnum, vbId);
        if (rdoc->deleted) {
            it->setDeleted();
        }

        /* update ep-engine IO stat read_bytes */
        st.io_read_bytes += rdoc->keylen;
    } else {
        size_t valuelen = rdoc->bodylen;
        void* valuePtr = rdoc->body;
        uint8_t ext_meta[EXT_META_LEN];

        if (checkUTF8JSON((const unsigned char *)valuePtr, valuelen)) {
            ext_meta[0] = PROTOCOL_BINARY_DATATYPE_JSON;
        } else {
            ext_meta[0] = PROTOCOL_BINARY_RAW_BYTES;
        }

        it = new Item((char *)rdoc->key, rdoc->keylen, forestMetaData.flags,
                      forestMetaData.exptime, valuePtr, valuelen,
                      ext_meta, EXT_META_LEN, forestMetaData.cas,
                      (uint64_t)rdoc->seqnum, vbId);

        /* update ep-engine IO stat read_bytes */
        st.io_read_bytes += (rdoc->keylen + valuelen);
    }

    it->setRevSeqno(forestMetaData.rev_seqno);

    /* increment ep-engine IO stat num_read */
    ++st.io_num_read;

    return GetValue(it);
}

vbucket_state* ForestKVStore::getVBucketState(uint16_t vbucketId) {
    return cachedVBStates[vbucketId];
}

void ForestKVStore::commitCallback(std::vector<ForestRequest *> &committedReqs) {
    size_t commitSize = committedReqs.size();

    for (size_t index = 0; index < commitSize; index++) {
        size_t dataSize = committedReqs[index]->getDataSize();
        size_t keySize = committedReqs[index]->getKey().length();
        /* update ep stats */
        ++st.io_num_write;
        st.io_write_bytes += (keySize + dataSize);

        int rv = committedReqs[index]->getStatus();
        if (committedReqs[index]->isDelete()) {
            if (rv != MUTATION_SUCCESS) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(committedReqs[index]->getDelta() / 1000);
                st.writeSizeHisto.add(keySize + FORESTDB_METADATA_SIZE);
            }
            committedReqs[index]->getDelCallback()->callback(rv);
        } else {
            if (rv != MUTATION_SUCCESS) {
               ++st.numSetFailure;
            } else {
               st.writeTimeHisto.add(committedReqs[index]->getDelta() / 1000);
               st.writeSizeHisto.add(dataSize + keySize);
            }
            //TODO: For now, all mutations are passed in as insertions.
            //This needs to be revisited in order to update stats.
            mutation_result p(rv, true);
            committedReqs[index]->getSetCallback()->callback(p);
        }
    }
}

bool ForestKVStore::save2forestdb() {
    size_t pendingCommitCnt = pendingReqsQ.size();
    if (pendingCommitCnt == 0) {
        return true;
    }

    uint16_t vbucket2flush = pendingReqsQ[0]->getVBucketId();

    vbucket_state* state = cachedVBStates[vbucket2flush];
    setVBucketState(vbucket2flush, state);

    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                           vbucket2flush,
                                                           HandleType::WRITER);

    hrtime_t start = gethrtime();
    fdb_status status = fdb_commit(fkvsHandle->getFileHandle(),
                                   FDB_COMMIT_NORMAL);
    if (status != FDB_RESULT_SUCCESS) {
        throw std::runtime_error("ForestKVStore::save2forestdb: "
            "fdb_commit failed for vbucket id: " + std::to_string(vbucket2flush) +
            "with error: " + std::string(fdb_error_msg(status)));
    }
    st.commitHisto.add((gethrtime() - start) / 1000);

    commitCallback(pendingReqsQ);

    st.batchSize.add(pendingCommitCnt);

    // clean up
    for (size_t i = 0; i < pendingCommitCnt; i++) {
        delete pendingReqsQ[i];
    }

    updateFileInfo(fkvsHandle.get(), vbucket2flush);

    pendingReqsQ.clear();

    st.docsCommitted = pendingCommitCnt;

    // Update cached blockCache hits and misses
    fdb_stats_cb_ctx ctx;
    status = fdb_fetch_handle_stats(fkvsHandle->getKvsHandle(),
                                    fdbStatsCallback,
                                    &ctx);
    if (status == FDB_RESULT_SUCCESS) {
        cachedBlockCacheHits[vbucket2flush] = ctx.stats["Block_cache_hits"];
        cachedBlockCacheMisses[vbucket2flush] = ctx.stats["Block_cache_misses"];
        cachedBlockCacheNumItems[vbucket2flush] = ctx.stats["Block_cache_num_items"];
        cachedBlockCacheNumVictims[vbucket2flush] = ctx.stats["Block_cache_num_victims"];
        cachedBlockCacheNumImmutables[vbucket2flush] = ctx.stats["Block_cache_num_immutables"];
    }

    return true;
}

bool ForestKVStore::commit() {
    if (intransaction) {
        if (save2forestdb()) {
            intransaction = false;
        }
    }

    return !intransaction;
}

StorageProperties ForestKVStore::getStorageProperties(void) {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::Yes,
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::Yes);
    return rv;
}

fdb_kvs_handle* ForestKVStore::openKvsHandle(fdb_file_handle& file_handle,
                                             uint16_t vbId, bool defaultKVS) {
    fdb_status status;
    fdb_kvs_handle* kvsHandle = nullptr;

    if (defaultKVS) {
        status = fdb_kvs_open_default(&file_handle, &kvsHandle, &kvsConfig);
    } else {
        char kvsName[20];
        int len = snprintf(kvsName, sizeof(kvsName), "vb%" PRIu16, vbId);
        if (len < 0) {
            throw std::runtime_error("ForestKVStore::openKvsHandle: Formatting "
                                     "error in generating the KVStore name for "
                                     "vb:" + std::to_string(vbId));
        }

        status = fdb_kvs_open(&file_handle, &kvsHandle, kvsName, &kvsConfig);
    }

    if (status != FDB_RESULT_SUCCESS) {
        throw std::runtime_error("ForestKVStore::openKvsHandle: Failed to "
                                 "create KVStore handle for vbucket:" +
                                 std::to_string(vbId));
    }

    status = fdb_set_log_callback(kvsHandle, errorlog_cb, nullptr);
    if (status != FDB_RESULT_SUCCESS) {
        throw std::runtime_error("ForestKVStore::openKvsHandle: Setting the "
                                 "log callback for KV Store instance failed "
                                 "with error:" +
                                 std::string(fdb_error_msg(status)));
    }

    return kvsHandle;
}

ForestKvsHandle* ForestKVStore::createFKvsHandle(uint16_t vbId, bool defaultKVS) {
    fdb_file_handle* newDBFileHandle = nullptr;
    fdb_kvs_handle* newKvsHandle = nullptr;
    fdb_status status;

    std::string dbFile = getDBFileName(dbname, vbId, dbFileRevMap[vbId]);

    status = fdb_open(&newDBFileHandle, dbFile.c_str(), &fileConfig);
    if (status != FDB_RESULT_SUCCESS) {
        throw std::runtime_error("ForestKVStore::createFKvsHandle: Opening a "
                                 "database file instance failed with error: " +
                                 std::string(fdb_error_msg(status)));
    }

    newKvsHandle = openKvsHandle(*newDBFileHandle, vbId, defaultKVS);

    return new ForestKvsHandle(newDBFileHandle, newKvsHandle);
}

std::shared_ptr<ForestKvsHandle> ForestKVStore::getOrCreateFKvsHandle(
                                                              uint16_t vbId,
                                                              HandleType type) {
    switch (type) {
        case HandleType::READER:
            if (!roFKvsHandleMap[vbId]) {
                std::unique_ptr<ForestKvsHandle> fkvs(createFKvsHandle(vbId));
                roFKvsHandleMap[vbId] = std::move(fkvs);
                st.numOpen++;
            }
            return roFKvsHandleMap[vbId];

        case HandleType::WRITER:
            if (!rwFKvsHandleMap[vbId]) {
                std::unique_ptr<ForestKvsHandle> fkvs(createFKvsHandle(vbId));
                rwFKvsHandleMap[vbId] = std::move(fkvs);
                st.numOpen++;
            }
            return rwFKvsHandleMap[vbId];

        case HandleType::STATE_SNAP:
            if (!stateFKvsHandleMap[vbId]) {
                std::unique_ptr<ForestKvsHandle> fkvs(createFKvsHandle(
                                                    vbId, true/*defaultKVS*/));
                stateFKvsHandleMap[vbId] = std::move(fkvs);
                st.numOpen++;
            }
            return stateFKvsHandleMap[vbId];
        default:
            throw std::logic_error("Unknown Handle type!");
    }
}

bool ForestKVStore::getStat(const char* name, size_t& value) {
    if (strcmp("Block_cache_hits", name) == 0) {
        size_t count = 0;
        for (uint16_t vbid = 0; vbid < numDbFiles; ++vbid) {
            count += cachedBlockCacheHits[vbid];
        }
        value = count;
        return true;
    } else if (strcmp("Block_cache_misses", name) == 0) {
        size_t count = 0;
        for (uint16_t vbid = 0; vbid < numDbFiles; ++vbid) {
            count += cachedBlockCacheMisses[vbid];
        }
        value = count;
        return true;
    } else if (strcmp("Block_cache_num_items", name) == 0) {
        size_t count = 0;
        for (uint16_t vbid = 0; vbid < numDbFiles; ++ vbid) {
            count += cachedBlockCacheNumItems[vbid];
        }
        value = count;
        return true;
    } else if (strcmp("Block_cache_num_victims", name) == 0) {
        size_t count = 0;
        for (uint16_t vbid = 0; vbid < numDbFiles; ++vbid) {
            count += cachedBlockCacheNumVictims[vbid];
        }
        value = count;
        return true;
    } else if (strcmp("Block_cache_num_immutables", name) == 0) {
        size_t count = 0;
        for (uint16_t vbid = 0; vbid < numDbFiles; ++vbid) {
            count += cachedBlockCacheNumImmutables[vbid];
        }
        value = count;
        return true;
    }

    return false;
}

static int8_t getMutationStatus(fdb_status errCode) {
    switch(errCode) {
        case FDB_RESULT_SUCCESS:
            return MUTATION_SUCCESS;
        case FDB_RESULT_NO_DB_HEADERS:
        case FDB_RESULT_NO_SUCH_FILE:
        case FDB_RESULT_KEY_NOT_FOUND:
            return DOC_NOT_FOUND;
        default:
            return MUTATION_FAILED;
    }
}

static void populateMetaData(const Item& itm, uint8_t* meta, bool deletion) {
    uint64_t cas = htonll(itm.getCas());
    uint64_t rev_seqno = htonll(itm.getRevSeqno());
    uint32_t flags = itm.getFlags();
    uint32_t exptime = itm.getExptime();
    uint32_t texptime = 0;

    if (deletion) {
        texptime = ep_real_time();
    }

    exptime = htonl(exptime);
    texptime = htonl(texptime);

    memcpy(meta, &cas, sizeof(cas));
    memcpy(meta + forestMetaOffset(rev_seqno), &rev_seqno, sizeof(rev_seqno));
    memcpy(meta + forestMetaOffset(exptime), &exptime, sizeof(exptime));
    memcpy(meta + forestMetaOffset(texptime), &texptime, sizeof(texptime));
    memcpy(meta + forestMetaOffset(flags), &flags, sizeof(flags));

    *(meta + forestMetaOffset(flex_meta)) = FLEX_META_CODE;

    if (deletion) {
        *(meta + forestMetaOffset(ext_meta)) = PROTOCOL_BINARY_RAW_BYTES;
    } else {
        memcpy(meta + forestMetaOffset(ext_meta), itm.getExtMeta(),
               itm.getExtMetaLen());
    }
}

void ForestKVStore::set(const Item& itm, Callback<mutation_result>& cb) {
    if (!intransaction) {
        throw std::invalid_argument("ForestKVStore::set: intransaction must be "
                                    "true to perform a set operation.");
    }
    MutationRequestCallback requestcb;

    // each req will be de-allocated after commit
    requestcb.setCb = &cb;
    ForestRequest *req = new ForestRequest(itm, requestcb, false,
                                           FORESTDB_METADATA_SIZE + itm.getNBytes());
    fdb_doc setDoc;
    fdb_status status;
    uint8_t meta[FORESTDB_METADATA_SIZE];

    memset(meta, 0, sizeof(meta));
    populateMetaData(itm, meta, false);

    setDoc.key = const_cast<char *>(itm.getKey().c_str());
    setDoc.keylen = itm.getNKey();
    setDoc.meta = meta;
    setDoc.metalen = sizeof(meta);
    setDoc.body = const_cast<char *>(itm.getData());
    setDoc.bodylen = itm.getNBytes();
    setDoc.deleted = false;

    fdb_doc_set_seqnum(&setDoc, itm.getBySeqno());
    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                            req->getVBucketId(), HandleType::WRITER);

    status = fdb_set(fkvsHandle->getKvsHandle(), &setDoc);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::set: fdb_set failed "
            "for key: %s and vbucketId: %" PRIu16 " with error: %s",
            req->getKey().c_str(), req->getVBucketId(), fdb_error_msg(status));
        req->setStatus(getMutationStatus(status));
    }
    setDoc.body = NULL;
    setDoc.bodylen = 0;

    pendingReqsQ.push_back(req);
}

void ForestKVStore::get(const std::string& key, uint16_t vb,
                        Callback<GetValue>& cb, bool fetchDelete) {

    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                        vb, HandleType::READER);
    getWithHeader(fkvsHandle->getKvsHandle(), key, vb, cb, fetchDelete);
}

ENGINE_ERROR_CODE
ForestKVStore::getAllKeys(uint16_t vbid,
                          const std::string& start_key,
                          uint32_t count,
                          std::shared_ptr<Callback<const DocKey&>> cb) {

    std::unique_ptr<ForestKvsHandle> fkvsHandle(createFKvsHandle(vbid));

    fdb_iterator* fdb_iter = NULL;
    fdb_status status = fdb_iterator_init(fkvsHandle->getKvsHandle(), &fdb_iter,
                                          start_key.c_str(), strlen(start_key.c_str()),
                                          NULL, 0, FDB_ITR_NO_DELETES);
    if (status != FDB_RESULT_SUCCESS) {
        throw std::runtime_error("ForestKVStore::getAllKeys: iterator "
                   "initalization failed for vbucket id " + std::to_string(vbid) +
                   " and start key:" + start_key.c_str());
    }

    fdb_doc* rdoc = NULL;
    status = fdb_doc_create(&rdoc, NULL, 0, NULL, 0, NULL, 0);
    if (status != FDB_RESULT_SUCCESS) {
       fdb_iterator_close(fdb_iter);
       throw std::runtime_error("ForestKVStore::getAllKeys: creating "
                  "the document failed for vbucket id:" +
                  std::to_string(vbid) + " with error:" +
                  fdb_error_msg(status));
    }

    rdoc->key = cb_malloc(MAX_KEY_LENGTH);
    rdoc->meta = cb_malloc(FORESTDB_METADATA_SIZE);

    for (uint32_t curr_count = 0; curr_count < count; curr_count++) {
        status = fdb_iterator_get_metaonly(fdb_iter, &rdoc);
        if (status != FDB_RESULT_SUCCESS) {
            fdb_doc_free(rdoc);
            fdb_iterator_close(fdb_iter);
            throw std::runtime_error("ForestKVStore::getAllKeys: iterator "
                       "get failed for vbucket id " + std::to_string(vbid) +
                       " and start key:" + start_key.c_str());
        }
        size_t keylen = static_cast<size_t>(rdoc->keylen);
        const uint8_t* key = reinterpret_cast<const uint8_t *>(rdoc->key);
        // Collection: Currently only create the key in the DefaultCollection
        cb->callback(DocKey(key, keylen, DocNamespace::DefaultCollection));

        if (fdb_iterator_next(fdb_iter) != FDB_RESULT_SUCCESS) {
            break;
        }
    }

    fdb_doc_free(rdoc);
    fdb_iterator_close(fdb_iter);

    return ENGINE_SUCCESS;
}

void ForestKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t& itms) {
    bool meta_only = true;
    vb_bgfetch_queue_t::iterator itr = itms.begin();
    for (; itr != itms.end(); ++itr) {
        vb_bgfetch_item_ctx_t& bg_itm_ctx = (*itr).second;
        meta_only = bg_itm_ctx.isMetaOnly;

        RememberingCallback<GetValue> gcb;
        if (meta_only) {
            gcb.val.setPartial();
        }

        const std::string& key = (*itr).first;
        get(key, vb, gcb);
        ENGINE_ERROR_CODE status = gcb.val.getStatus();
        if (status != ENGINE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::getMulti: Failed to "
                "retrieve key: %s", key.c_str());
        }

        std::list<VBucketBGFetchItem *>& fetches = bg_itm_ctx.bgfetched_list;
        std::list<VBucketBGFetchItem *>:: iterator fitr = fetches.begin();

        for (fitr = fetches.begin(); fitr != fetches.end(); ++fitr) {
            (*fitr)->value = gcb.val;
            st.readTimeHisto.add((gethrtime() - (*fitr)->initTime) / 1000);
        }

        if (status == ENGINE_SUCCESS) {
            st.readSizeHisto.add(gcb.val.getValue()->getNKey() +
                                 gcb.val.getValue()->getNBytes());
        }
    }

    // Update cached blockCache hits and misses
    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                    vb, HandleType::READER);

    fdb_stats_cb_ctx ctx;
    fdb_status status = fdb_fetch_handle_stats(fkvsHandle->getKvsHandle(),
                                               fdbStatsCallback,
                                               &ctx);
    if (status == FDB_RESULT_SUCCESS) {
        cachedBlockCacheHits[vb] = ctx.stats["Block_cache_hits"];
        cachedBlockCacheMisses[vb] = ctx.stats["Block_cache_misses"];
        cachedBlockCacheNumItems[vb] = ctx.stats["Block_cache_num_items"];
        cachedBlockCacheNumVictims[vb] = ctx.stats["Block_cache_num_victims"];
        cachedBlockCacheNumImmutables[vb] = ctx.stats["Block_cache_num_immutables"];
    }
}

void ForestKVStore::del(const Item& itm, Callback<int>& cb) {
    if (!intransaction) {
        throw std::invalid_argument("ForestKVStore::del: intransaction must be "
                                    "true to perform a delete operation.");
    }
    MutationRequestCallback requestcb;
    requestcb.delCb = &cb;
    ForestRequest* req = new ForestRequest(itm, requestcb, true);
    fdb_doc delDoc;
    fdb_status status;
    uint8_t meta[FORESTDB_METADATA_SIZE];

    memset(meta, 0, sizeof(meta));
    populateMetaData(itm, meta, true);

    delDoc.key = const_cast<char *>(itm.getKey().c_str());
    delDoc.keylen = itm.getNKey();
    delDoc.meta = meta;
    delDoc.metalen = sizeof(meta);
    delDoc.deleted = true;

    fdb_doc_set_seqnum(&delDoc, itm.getBySeqno());
    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                            req->getVBucketId(),
                                                            HandleType::WRITER);
    status = fdb_del(fkvsHandle->getKvsHandle(), &delDoc);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "ForesKVStore::del: fdb_del failed "
            "for key: %s and vbucketId: %" PRIu16 " with error: %s",
            req->getKey().c_str(), req->getVBucketId(), fdb_error_msg(status));
        req->setStatus(getMutationStatus(status));
    }

    pendingReqsQ.push_back(req);
}

std::vector<vbucket_state *> ForestKVStore::listPersistedVbuckets(void) {
    return cachedVBStates;
}

size_t ForestKVStore::getNumPersistedDeletes(uint16_t vbid) {
    size_t delCount = cachedDeleteCount[vbid];
    if (delCount != (size_t) -1) {
        return delCount;
    }

    std::unique_ptr<ForestKvsHandle> fkvsHandle(createFKvsHandle(vbid));

    fdb_kvs_info kvsInfo;
    fdb_status status = fdb_get_kvs_info(fkvsHandle->getKvsHandle(), &kvsInfo);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::getNumPersistedDeletes:Failed to "
            "retrieve KV store info with error:" +
            std::string(fdb_error_msg(status)) + " for vbucket id:" +
            std::to_string(static_cast<int>(vbid)));
        throw std::runtime_error(err);
    }

    return kvsInfo.deleted_count;
}

DBFileInfo ForestKVStore::getDbFileInfo(uint16_t vbId) {
    DBFileInfo dbInfo;

    dbInfo.fileSize =  cachedFileSize[vbId].load();
    dbInfo.spaceUsed = cachedSpaceUsed[vbId].load();

    return dbInfo;
}

DBFileInfo ForestKVStore::getAggrDbFileInfo() {
    DBFileInfo dbInfo;
    /**
     * Iterate over all the vbuckets to get the total.
     * If the vbucket is dead, then its value would
     * be zero.
     */
    for (uint16_t vbid = 0; vbid < numDbFiles; vbid++) {
        dbInfo.fileSize += cachedFileSize[vbid].load();
        dbInfo.spaceUsed += cachedSpaceUsed[vbid].load();
    }

    return dbInfo;
}

bool ForestKVStore::snapshotVBucket(uint16_t vbucketId,
                                    const vbucket_state& vbstate,
                                    VBStatePersist options) {
    fdb_status status;
    hrtime_t start = gethrtime();

    if (updateCachedVBState(vbucketId, vbstate) &&
        (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
         options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {

        setVBucketState(vbucketId, cachedVBStates[vbucketId]);

        std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                           vbucketId, HandleType::STATE_SNAP);

        if (options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT) {
            status = fdb_commit(fkvsHandle->getFileHandle(), FDB_COMMIT_NORMAL);

            if (status != FDB_RESULT_SUCCESS) {
                LOG(EXTENSION_LOG_WARNING, "ForestKVStore::snapshotVBucket: Failed "
                    "to commit vbucket state for vbucket: %" PRIu16 " with error: %s",
                    vbucketId, fdb_error_msg(status));
                return false;
            }
        }

        updateFileInfo(fkvsHandle.get(), vbucketId);
    }

    st.snapshotHisto.add((gethrtime() - start) / 1000);

    return true;
}

ScanContext* ForestKVStore::initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                           std::shared_ptr<Callback<CacheLookup> > cl,
                                           uint16_t vbid, uint64_t startSeqno,
                                           DocumentFilter options,
                                           ValueFilter valOptions) {
    std::unique_ptr<ForestKvsHandle> fkvsHandle(createFKvsHandle(vbid));

    fdb_kvs_info kvsInfo;
    fdb_status status = fdb_get_kvs_info(fkvsHandle->getKvsHandle(), &kvsInfo);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::initScanContext: Failed to retrieve "
            "KV store info with error:" + std::string(fdb_error_msg(status)) +
            "vbucket id:" + std::to_string(static_cast<int>(vbid)));
        throw std::runtime_error(err);
    }

    size_t scanId = scanCounter++;
    size_t count = getNumItems(fkvsHandle->getKvsHandle(),
                               startSeqno,
                               std::numeric_limits<uint64_t>::max());

    {
        LockHolder lh(scanLock);
        scans[scanId] = std::move(fkvsHandle);
    }

    return new ScanContext(cb, cl, vbid, scanId, startSeqno,
                           (uint64_t)kvsInfo.last_seqnum, options,
                           valOptions, count);
}

scan_error_t ForestKVStore::scan(ScanContext* ctx) {
    if (!ctx) {
        return scan_failed;
    }

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        return scan_success;
    }

    fdb_kvs_handle* kvsHandle = nullptr;
    {
        LockHolder lh(scanLock);
        auto itr = scans.find(ctx->scanId);
        if (itr == scans.end()) {
            return scan_failed;
        }

        kvsHandle = (itr->second)->getKvsHandle();
    }

    fdb_iterator_opt_t options;

    switch (ctx->docFilter) {
        case DocumentFilter::NO_DELETES:
            options = FDB_ITR_NO_DELETES;
            break;
        case DocumentFilter::ALL_ITEMS:
            options = FDB_ITR_NONE;
            break;
        default:
            std::string err("ForestKVStore::scan: Illegal document filter!" +
                            std::to_string(static_cast<int>(ctx->docFilter)));
            throw std::logic_error(err);
    }

    switch (ctx->valFilter) {
        case ValueFilter::KEYS_ONLY:
            options |= FDB_ITR_NO_VALUES;
            break;
        case ValueFilter::VALUES_COMPRESSED:
            // TODO: NOT SUPPORTED YET (MB-19682)
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::scan: "
                "Getting compressed data - Not supported yet with forestdb");
            return scan_failed;
        case ValueFilter::VALUES_DECOMPRESSED:
            break;
        default:
            std::string err("ForestKVStore::scan: Illegal value filter!" +
                            std::to_string(static_cast<int>(ctx->valFilter)));
            throw std::logic_error(err);
    }

    fdb_seqnum_t start = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        start = static_cast<fdb_seqnum_t>(ctx->lastReadSeqno) + 1;
    }

    fdb_status status = fdb_changes_since(kvsHandle, start, options,
            recordChanges,
            static_cast<void*>(ctx));

    switch (status) {
        case FDB_RESULT_SUCCESS:
            /* Success */
            break;
        case FDB_RESULT_CANCELLED:
            /* Retry */
            return scan_again;
        default:
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::scan: "
                    "fdb_changes_since api failed, error: %s",
                    fdb_error_msg(status));
            return scan_failed;
    }

    return scan_success;
}

void ForestKVStore::destroyScanContext(ScanContext* ctx) {
    if (!ctx) {
        return;
    }

    LockHolder lh(scanLock);
    auto itr = scans.find(ctx->scanId);
    if (itr != scans.end()) {
        scans.erase(itr);
    }

    delete ctx;
}

fdb_compact_decision ForestKVStore::compaction_cb(fdb_file_handle* fhandle,
                                                  fdb_compaction_status comp_status,
                                                  const char* kv_name,
                                                  const fdb_doc* doc, uint64_t old_offset,
                                                  uint64_t new_offset, void* ctx) {
    /**
     * The default KV store holds the vbucket state information for all the vbuckets
     * in the shard.
     */
    if (strcmp(kv_name, "default") == 0) {
        return FDB_CS_KEEP_DOC;
    }

    compaction_ctx* comp_ctx = reinterpret_cast<compaction_ctx *>(ctx);
    ForestKVStore* store = reinterpret_cast<ForestKVStore *>(comp_ctx->store);

    /* Every KV store name for ep-engine has the format "vb<vbucket id>."
     * Extract the vbucket id from the kvstore name */
    std::string kvNameStr(kv_name);
    kvNameStr.erase(kvNameStr.begin(), kvNameStr.begin() + strlen("vb"));

    uint16_t vbid;
    try {
        vbid = std::stoi(kvNameStr);
    } catch (...) {
        LOG(EXTENSION_LOG_WARNING,
            "ForestKVStore::compaction_cb: Failed to convert string to integer "
            "for KV store name: %s", kv_name);
        return FDB_CS_KEEP_DOC;
    }

    fdb_seqnum_t lastSeqnum;
    fdb_kvs_handle* kvsHandle = store->getCompactionCbCtxHandle(vbid);
    if (kvsHandle) {
        fdb_status status = fdb_get_kvs_seqnum(kvsHandle, &lastSeqnum);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "ForestKVStore::compaction_cb: Failed to retrieve KV store "
                "information for vbucket %" PRIu16 " with error: %s",
                vbid, fdb_error_msg(status));
            return FDB_CS_KEEP_DOC;
        }
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "ForestKVStore::compaction_cb: Failed as compactionCtxHandle "
            "wasn't set! vb: %" PRIu16, vbid);
        return FDB_CS_KEEP_DOC;
    }

    ForestMetaData forestMetaData = forestMetaDecode(doc);

    std::string key((char *)doc->key, doc->keylen);
    if (doc->deleted) {
        uint64_t max_purge_seq = 0;
        auto it = comp_ctx->max_purged_seq.find(vbid);

        if (it == comp_ctx->max_purged_seq.end()) {
            comp_ctx->max_purged_seq[vbid] = 0;
        } else {
            max_purge_seq = it->second;
        }

        if (doc->seqnum != lastSeqnum) {
            if (comp_ctx->drop_deletes) {
                if (max_purge_seq < doc->seqnum) {
                    comp_ctx->max_purged_seq[vbid] = doc->seqnum;
                }

                return FDB_CS_DROP_DOC;
            }

            if (forestMetaData.texptime < comp_ctx->purge_before_ts &&
                    (!comp_ctx->purge_before_seq ||
                     doc->seqnum <= comp_ctx->purge_before_seq)) {
                if (max_purge_seq < doc->seqnum) {
                    comp_ctx->max_purged_seq[vbid] = doc->seqnum;
                }

                return FDB_CS_DROP_DOC;
            }
        }
    } else if (forestMetaData.exptime) {
        std::vector<vbucket_state *> vbStates = store->listPersistedVbuckets();
        vbucket_state* vbState = vbStates[vbid];
        if (vbState && vbState->state == vbucket_state_active) {
            time_t curr_time = ep_real_time();
            if (forestMetaData.exptime < curr_time) {
                comp_ctx->expiryCallback->callback(vbid, key,
                                                   forestMetaData.rev_seqno,
                                                   curr_time);
            }
        }
    }

    if (comp_ctx->bloomFilterCallback) {
        bool deleted = doc->deleted;
        comp_ctx->bloomFilterCallback->callback(vbid, key, deleted);
    }

    return FDB_CS_KEEP_DOC;
}

bool ForestKVStore::compactDB(compaction_ctx* ctx) {
    hrtime_t start = gethrtime();
    uint16_t vbucketId = ctx->db_file_id;
    uint64_t fileRev = dbFileRevMap[vbucketId];
    fdb_status status;

    std::string dbFileBase = dbname + "/" + std::to_string(vbucketId) + ".fdb.";
    std::string prevDbFile = dbFileBase + std::to_string(fileRev);
    std::string newDbFile = dbFileBase + std::to_string(fileRev + 1);

    fileConfig.compaction_cb = compaction_cb_c;
    fileConfig.compaction_cb_ctx = ctx;
    fileConfig.compaction_cb_mask = FDB_CS_MOVE_DOC;

    ctx->store = this;

    std::unique_ptr<ForestKvsHandle> compactHandle;
    try {
        std::unique_ptr<ForestKvsHandle> temp(createFKvsHandle(vbucketId));
        compactHandle = std::move(temp);
    } catch (std::runtime_error& e) {
        logger.log(EXTENSION_LOG_WARNING,
                   "ForestKVStore::compactDB: Failed to open file: %s [%s]",
                   prevDbFile.c_str(), e.what());
        return false;
    }

    // Update CompactionCbCtx handle
    setCompactionCbCtxHandle(vbucketId, compactHandle->getKvsHandle());

    status = fdb_compact(compactHandle->getFileHandle(), newDbFile.c_str());
    if (status != FDB_RESULT_SUCCESS) {
        logger.log(EXTENSION_LOG_WARNING,
                   "ForestKVStore::compactDB: Failed to compact from database "
                   "file: %s to database file: %s with error: %s",
                   prevDbFile.c_str(), newDbFile.c_str(), fdb_error_msg(status));
        return false;
    }

    // Update the global VBucket file map so all operations use the new file
    updateDbFileMap(vbucketId, fileRev + 1);

    // Upon successful compaction, the previously opened file handle is
    // automatically re-directed to the new file.

    updateFileInfo(compactHandle.get(), vbucketId);

    st.compactHisto.add((gethrtime() - start) / 1000);

    // Reset CompactionCbCtx handle
    setCompactionCbCtxHandle(vbucketId, nullptr);

    return true;
}

size_t ForestKVStore::getNumItems(uint16_t vbid, uint64_t min_seq,
                                  uint64_t max_seq) {
    std::unique_ptr<ForestKvsHandle> fkvsHandle(createFKvsHandle(vbid));
    return getNumItems(fkvsHandle->getKvsHandle(), min_seq, max_seq);
}

size_t ForestKVStore::getNumItems(fdb_kvs_handle* kvsHandle,
                                  uint64_t min_seq,
                                  uint64_t max_seq) {
    // TODO: Replace this API's content with fdb_changes_count(),
    // needs MB-16563.
    size_t totalCount = 0;
    fdb_iterator* fdb_iter = nullptr;
    fdb_status status = fdb_iterator_sequence_init(kvsHandle, &fdb_iter, min_seq,
                                                   max_seq, FDB_ITR_NONE);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::getNumItems: ForestDB iterator "
            "initialization failed with error: " +
             std::string(fdb_error_msg(status)));
        throw std::runtime_error(err);
    }

    do {
        totalCount++;
    } while (fdb_iterator_next(fdb_iter) == FDB_RESULT_SUCCESS);

    fdb_iterator_close(fdb_iter);

    return totalCount;
}

size_t ForestKVStore::getItemCount(uint16_t vbid) {
    if (cachedDocCount.at(vbid) == static_cast<size_t>(-1)) {
        fdb_status status;
        fdb_kvs_info kvsInfo;

        std::unique_ptr<ForestKvsHandle> fkvsHandle(createFKvsHandle(vbid));

        status = fdb_get_kvs_info(fkvsHandle->getKvsHandle(), &kvsInfo);
        if (status != FDB_RESULT_SUCCESS) {
            std::string err("ForestKVStore::getItemCount::Failed to retrieve "
                            "KV store info with error:" +
                            std::string(fdb_error_msg(status)) +
                            " for vbucket id:" +
                            std::to_string(static_cast<int>(vbid)));
            throw std::runtime_error(err);
        }

        cachedDocCount[vbid] = kvsInfo.doc_count;
    }

    return cachedDocCount.at(vbid);
}

RollbackResult ForestKVStore::rollback(uint16_t vbid, uint64_t rollbackSeqno,
                                       std::shared_ptr<RollbackCB> cb) {

    std::shared_ptr<ForestKvsHandle> fkvsHandle = getOrCreateFKvsHandle(
                                                      vbid, HandleType::WRITER);
    fdb_kvs_handle* kvsHandle = fkvsHandle->getKvsHandle();

    fdb_kvs_info kvsInfo;
    fdb_status status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::rollback: "
            "Failed to retrieve KV store info with error: %s for "
            "vbucket: %" PRIu16 " and rollback sequence number: %" PRIu64,
            fdb_error_msg(status), vbid, rollbackSeqno);
        return RollbackResult(false, 0, 0 ,0);
    }

    // Get closest available sequence number to rollback to
    uint64_t currentSeqno = fdb_get_available_rollback_seq(kvsHandle,
                                                           rollbackSeqno);
    if (currentSeqno == 0) {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::rollback: "
            "Unable to find an available rollback sequence number "
            "for vbucket: %" PRIu16 " with rollback request sequence number:"
            " %" PRIu64, vbid, rollbackSeqno);
        return RollbackResult(false, 0, 0, 0);
    }

    // Create a new snap handle for the persisted snapshot up till the
    // point of rollback. This snapshot is needed to identify the earlier
    // revision of the items that are being rolled back.
    fdb_kvs_handle* snaphandle = NULL;
    status = fdb_snapshot_open(kvsHandle, &snaphandle, currentSeqno);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::rollback: "
            "Failed to retrieve persisted snapshot handle from the "
            "kvs handle, error: %s for "
            "vbucket: %" PRIu16 " and snapshot sequence number: %" PRIu64,
            fdb_error_msg(status), vbid, currentSeqno + 1);
        return RollbackResult(false, 0, 0 ,0);
    }

    // Set snaphandle as the callback's handle
    cb->setDbHeader(snaphandle);

    std::shared_ptr<Callback<CacheLookup> > cl(new NoLookupCallback);
    ScanContext* ctx = initScanContext(cb, cl, vbid, currentSeqno,
                                       DocumentFilter::ALL_ITEMS,
                                       ValueFilter::KEYS_ONLY);
    scan_error_t error = scan(ctx);
    destroyScanContext(ctx);

    // Close snap handle
    fdb_kvs_close(snaphandle);

    if (error != scan_success) {
        return RollbackResult(false, 0, 0, 0);
    }

    // Initiate disk rollback
    status = fdb_rollback(&kvsHandle, currentSeqno);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::rollback: "
            "ForestDB rollback failed on vbucket: %" PRIu16 " and rollback "
            "sequence number: %" PRIu64 "with error: %s\n",
            vbid, currentSeqno, fdb_error_msg(status));
        return RollbackResult(false, 0, 0, 0);
    }

    status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::rollback: "
            "Failed to retrieve KV store info after rollback with error: %s "
            "for vbucket: %" PRIu16 " and rollback sequence number: %" PRIu64,
            fdb_error_msg(status), vbid, rollbackSeqno);
        return RollbackResult(false, 0, 0 ,0);
    }

    readVBState(vbid);

    cachedDocCount[vbid] = kvsInfo.doc_count;
    cachedDeleteCount[vbid] = kvsInfo.deleted_count;

    vbucket_state *vb_state = cachedVBStates[vbid];
    return RollbackResult(true, vb_state->highSeqno,
                          vb_state->lastSnapStart, vb_state->lastSnapEnd);
}

void ForestKVStore::pendingTasks() {
    fdb_status status;
    if (!pendingFileDeletions.empty()) {
        std::queue<std::string> queue;
        pendingFileDeletions.getAll(queue);

        while (!queue.empty()) {
            std::string filename_str = queue.front();
            status = fdb_destroy(filename_str.c_str(), &fileConfig);
            if (status != FDB_RESULT_SUCCESS) {
                logger.log(EXTENSION_LOG_WARNING, "Failed to destroy file '%s' "
                           "with status: %s, erno: %d", filename_str.c_str(),
                           fdb_error_msg(status), errno);
                if (errno != ENOENT) {
                    pendingFileDeletions.push(filename_str);
                }
            }
            queue.pop();
        }
    }
}

extern "C" {
    static fdb_fileops_handle ffs_constructor(void *ctx);
    static fdb_status ffs_open(const char *pathname, fdb_fileops_handle *, int flags,
                               mode_t mode);
    static fdb_ssize_t ffs_pwrite(fdb_fileops_handle fops_handle, void *buf,
                                  size_t count, cs_off_t offset);
    static fdb_ssize_t ffs_pread(fdb_fileops_handle fops_handle, void *buf,
                                 size_t count, cs_off_t offset);
    static int ffs_close(fdb_fileops_handle fops_handle);
    static cs_off_t ffs_goto_eof(fdb_fileops_handle fops_handle);
    static cs_off_t ffs_file_size(fdb_fileops_handle fops_handle, const char *filename);
    static int ffs_fdatasync(fdb_fileops_handle fops_handle);
    static int ffs_fsync(fdb_fileops_handle fops_handle);
    static void ffs_get_errno_str(fdb_fileops_handle fops_handle, char *buf, size_t size);
    static void* ffs_mmap(fdb_fileops_handle fops_handle,
                          size_t length, void **aux);
    static int ffs_munmap(fdb_fileops_handle fops_handle,
                          void *addr, size_t length, void *aux);

    static int ffs_aio_init(fdb_fileops_handle fops_handle, struct async_io_handle *aio_handle);
    static int ffs_aio_prep_read(fdb_fileops_handle fops_handle,
                                 struct async_io_handle *aio_handle, size_t aio_idx,
                                 size_t read_size, uint64_t offset);
    static int ffs_aio_submit(fdb_fileops_handle fops_handle,
                              struct async_io_handle *aio_handle, int num_subs);
    static int ffs_aio_getevents(fdb_fileops_handle fops_handle,
                                 struct async_io_handle *aio_handle, int min,
                                 int max, unsigned int timeout);
    static int ffs_aio_destroy(fdb_fileops_handle fops_handle,
                               struct async_io_handle *aio_handle);
    static int ffs_get_fs_type(fdb_fileops_handle src_fops_handle);
    static int ffs_copy_file_range(int fs_type, fdb_fileops_handle src_fops_handle,
                                   fdb_fileops_handle dst_fops_handle,
                                   uint64_t src_off, uint64_t dst_off, uint64_t len);
    static void ffs_destructor(fdb_fileops_handle fops_handle);
}

fdb_filemgr_ops_t ForestKVStore::getForestStatOps(FileStats* stats) {
    fdb_filemgr_ops_t fileOps = {
        ffs_constructor,
        ffs_open,
        ffs_pwrite,
        ffs_pread,
        ffs_close,
        ffs_goto_eof,
        ffs_file_size,
        ffs_fdatasync,
        ffs_fsync,
        ffs_get_errno_str,
        ffs_mmap,
        ffs_munmap,
        ffs_aio_init,
        ffs_aio_prep_read,
        ffs_aio_submit,
        ffs_aio_getevents,
        ffs_aio_destroy,
        ffs_get_fs_type,
        ffs_copy_file_range,
        ffs_destructor,
        stats,
    };

    return fileOps;
}

struct ForestStatFile {
    ForestStatFile(fdb_filemgr_ops_t *_orig_ops, fdb_fileops_handle _orig_handle,
                   cs_off_t _last_offs) {
        orig_ops = _orig_ops;
        orig_handle = _orig_handle;
        last_offs = _last_offs;
    }
    FileStats *fs_stats;
    const fdb_filemgr_ops_t *orig_ops;
    fdb_fileops_handle orig_handle;
    cs_off_t last_offs;
};

extern "C" {
    static fdb_fileops_handle ffs_constructor(void *ctx) {
        fdb_filemgr_ops_t *orig_ops = fdb_get_default_file_ops();
        ForestStatFile *fsf = new ForestStatFile(orig_ops,
                                                 orig_ops->constructor(orig_ops->ctx), 0);
        fsf->fs_stats = static_cast<FileStats *>(ctx);
        return reinterpret_cast<fdb_fileops_handle>(fsf);
    }

    static fdb_status ffs_open(const char *pathname, fdb_fileops_handle *fops_handle,
                               int flags, mode_t mode) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(*fops_handle);
        if (fsf) {
            return fsf->orig_ops->open(pathname, &fsf->orig_handle, flags, mode);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static fdb_ssize_t ffs_pwrite(fdb_fileops_handle fops_handle, void *buf,
                                  size_t count, cs_off_t offset) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            fsf->fs_stats->writeSizeHisto.add(count);
            BlockTimer bt(&fsf->fs_stats->writeTimeHisto);
            ssize_t result = fsf->orig_ops->pwrite(fsf->orig_handle, buf, count,
                                                   offset);
            if (result > 0) {
                fsf->fs_stats->totalBytesWritten += result;
            }

            return result;
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static fdb_ssize_t ffs_pread(fdb_fileops_handle fops_handle, void *buf,
                                 size_t count, cs_off_t offset) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);

        if (fsf) {
            fsf->fs_stats->readSizeHisto.add(count);
            if (fsf->last_offs) {
                fsf->fs_stats->readSeekHisto.add(std::abs(offset - fsf->last_offs));
            }

            fsf->last_offs = offset;
            BlockTimer bt(&fsf->fs_stats->readTimeHisto);
            ssize_t result = fsf->orig_ops->pread(fsf->orig_handle, buf, count,
                                                  offset);
            if (result) {
                fsf->fs_stats->totalBytesRead += result;
            }

            return result;
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_close(fdb_fileops_handle fops_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->close(fsf->orig_handle);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static cs_off_t ffs_goto_eof(fdb_fileops_handle fops_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->goto_eof(fsf->orig_handle);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static cs_off_t ffs_file_size(fdb_fileops_handle fops_handle, const char *filename) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->file_size(fsf->orig_handle, filename);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_fdatasync(fdb_fileops_handle fops_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->fdatasync(fsf->orig_handle);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_fsync(fdb_fileops_handle fops_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            BlockTimer bt(&fsf->fs_stats->syncTimeHisto);
            return fsf->orig_ops->fsync(fsf->orig_handle);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static void ffs_get_errno_str(fdb_fileops_handle fops_handle, char *buf, size_t size) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            fsf->orig_ops->get_errno_str(fsf->orig_handle, buf, size);
        }
    }

    static void* ffs_mmap(fdb_fileops_handle fops_handle,
                          size_t length, void **aux) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->mmap(fsf->orig_handle, length, aux);
        }
        return nullptr;
    }

    static int ffs_munmap(fdb_fileops_handle fops_handle,
                          void *addr, size_t length, void *aux) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->munmap(fsf->orig_handle, addr, length, aux);
        }
        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_aio_init(fdb_fileops_handle fops_handle,
                            struct async_io_handle *aio_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->aio_init(fsf->orig_handle, aio_handle);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_aio_prep_read(fdb_fileops_handle fops_handle,
                                 struct async_io_handle *aio_handle,
                                 size_t aio_idx, size_t read_size,
                                 uint64_t offset) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->aio_prep_read(fsf->orig_handle, aio_handle,
                                                aio_idx, read_size, offset);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_aio_submit(fdb_fileops_handle fops_handle,
                              struct async_io_handle *aio_handle,
                              int num_subs) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->aio_submit(fsf->orig_handle, aio_handle,
                                             num_subs);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_aio_getevents(fdb_fileops_handle fops_handle,
                                 struct async_io_handle *aio_handle,
                                 int min, int max, unsigned int timeout) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->aio_getevents(fsf->orig_handle,
                                                aio_handle, min, max, timeout);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_aio_destroy(fdb_fileops_handle fops_handle,
                               struct async_io_handle *aio_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        if (fsf) {
            return fsf->orig_ops->aio_destroy(fsf->orig_handle,
                                              aio_handle);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_get_fs_type(fdb_fileops_handle src_fileops_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(src_fileops_handle);
        if (fsf) {
            return fsf->orig_ops->get_fs_type(fsf->orig_handle);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static int ffs_copy_file_range(int fs_type,
                                   fdb_fileops_handle src_fops_handle,
                                   fdb_fileops_handle dst_fops_handle,
                                   uint64_t src_off,
                                   uint64_t dst_off,
                                   uint64_t len) {
        ForestStatFile *src_fsf = reinterpret_cast<ForestStatFile *>(src_fops_handle);
        ForestStatFile *dst_fsf = reinterpret_cast<ForestStatFile *>(dst_fops_handle);

        if (src_fsf && dst_fsf) {
            return src_fsf->orig_ops->copy_file_range(fs_type, src_fsf->orig_handle,
                                                      dst_fsf->orig_handle,
                                                      src_off, dst_off, len);
        }

        return FDB_RESULT_INVALID_ARGS;
    }

    static void ffs_destructor(fdb_fileops_handle fops_handle) {
        ForestStatFile *fsf = reinterpret_cast<ForestStatFile *>(fops_handle);
        fsf->orig_ops->destructor(fsf->orig_handle);
        delete fsf;
    }
}
