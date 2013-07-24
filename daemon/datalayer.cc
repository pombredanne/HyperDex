// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#define __STDC_LIMIT_MACROS

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// POSIX
#include <signal.h>

// STL
#include <algorithm>
#include <sstream>
#include <string>

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/datatypes.h"
#include "common/macros.h"
#include "common/range_searches.h"
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/datalayer.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_iterator.h"

// ASSUME:  all keys put into db have a first byte without the high bit set

using hyperdex::datalayer;
using hyperdex::reconfigure_returncode;

datalayer :: datalayer(daemon* d)
    : m_daemon(d)
    , m_db()
    , m_counters()
    , m_cleaner(std::tr1::bind(&datalayer::cleaner, this))
    , m_block_cleaner()
    , m_wakeup_cleaner(&m_block_cleaner)
    , m_wakeup_reconfigurer(&m_block_cleaner)
    , m_need_cleaning(false)
    , m_shutdown(true)
    , m_need_pause(false)
    , m_paused(false)
    , m_state_transfer_captures()
{
}

datalayer :: ~datalayer() throw ()
{
    shutdown();
}

bool
datalayer :: setup(const po6::pathname& path,
                   bool* saved,
                   server_id* saved_us,
                   po6::net::location* saved_bind_to,
                   po6::net::hostname* saved_coordinator,
				   unsigned threads,
				   unsigned maxsize)
{
	size_t msize = (size_t)maxsize * 1024ULL * 1024ULL;
	MDB_txn *txn;
	MDB_val key, val;
	int rc;
	bool ret = false;

	rc = mdb_env_create(&m_db);
	if (rc)
	{
        LOG(ERROR) << "could not create LMDB env: " << mdb_strerror(rc);
        return false;
	}
	rc = mdb_env_set_mapsize(m_db, msize);
	rc = mdb_env_open(m_db, path.get(), MDB_WRITEMAP|MDB_NOMETASYNC, 0600);
	if (rc)
	{
        LOG(ERROR) << "could not open LMDB env: " << mdb_strerror(rc);
        return false;
	}
	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
        LOG(ERROR) << "could not open LMDB txn: " << mdb_strerror(rc);
        return false;
	}
	rc = mdb_dbi_open(txn, NULL, 0, &m_dbi);
	if (rc)
	{
        LOG(ERROR) << "could not open LMDB dbi: " << mdb_strerror(rc);
		mdb_txn_abort(txn);
        return false;
	}

	// read the "hyperdex" key and check the version
    bool first_time = false;

	MVS(key, "hyperdex");
	rc = mdb_get(txn, m_dbi, &key, &val);

    if (rc == MDB_SUCCESS)
    {
        first_time = false;

		if (memcmp(val.mv_data, MVAL(PACKAGE_VERSION)) &&
			memcmp(val.mv_data, MVAL("1.0.rc1")) &&
			memcmp(val.mv_data, MVAL("1.0.rc2")))
        {
            LOG(ERROR) << "could not restore from DB because "
                       << "the existing data was created by "
                       << "HyperDex " << val.mv_data << " but "
                       << "this is version " << PACKAGE_VERSION;
            goto leave;
        }
    }
    else if (rc == MDB_NOTFOUND)
    {
        first_time = true;
    }
    else
    {
        LOG(ERROR) << "could not restore from DB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << mdb_strerror(rc);
        goto leave;
    }

	MVS(key, "state");
	rc = mdb_get(txn, m_dbi, &key, &val);
    if (rc == MDB_SUCCESS)
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from DB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you're on your own with this one";
            goto leave;
        }
    }
    else if (rc == MDB_NOTFOUND)
    {
        if (!first_time)
        {
            LOG(ERROR) << "could not restore from DB because a previous "
                       << "execution crashed; run the recovery program and try again";
            goto leave;
        }
    }
    else
    {
        LOG(ERROR) << "could not restore from DB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << mdb_strerror(rc);
        goto leave;
    }

    {
        po6::threads::mutex::hold hold(&m_block_cleaner);
        m_cleaner.start();
        m_shutdown = false;
    }

    if (first_time)
    {
        *saved = false;
		ret = true;
		goto leave;
    }

	{
    uint64_t us;
    *saved = true;
    e::unpacker up((const char *)val.mv_data, val.mv_size);
    up = up >> us >> *saved_bind_to >> *saved_coordinator;
    *saved_us = server_id(us);

    if (up.error())
    {
        LOG(ERROR) << "could not restore from DB because a previous "
                   << "execution saved invalid state; run the recovery program and try again";
        goto leave;
    }
	}
	ret = true;

leave:
	mdb_txn_commit(txn);
    return ret;
}

void
datalayer :: teardown()
{
    shutdown();
}

bool
datalayer :: initialize()
{
	MDB_txn *txn;
	MDB_val key, val;
	int rc;

	MVS(key, "hyperdex");
	MVS(val, PACKAGE_VERSION);

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
		LOG(ERROR) << "could not open txn: " << mdb_strerror(rc);
		return false;
	}
	rc = mdb_put(txn, m_dbi, &key, &val, 0);
	if (rc == MDB_SUCCESS) {
		rc = mdb_txn_commit(txn);
		if (rc == MDB_SUCCESS)
			return true;
		txn = NULL;
	}

    LOG(ERROR) << "could not initialize DB: " << mdb_strerror(rc);
	if (txn)
		mdb_txn_abort(txn);
    return false;
}

bool
datalayer :: save_state(const server_id& us,
                        const po6::net::location& bind_to,
                        const po6::net::hostname& coordinator)
{
	MDB_txn *txn;
	MDB_val key, val;
	int rc;

	MVS(key, "dirty");
	MVS(val, "");

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
		LOG(ERROR) << "could not open txn: " << mdb_strerror(rc);
		return false;
	}
	rc = mdb_put(txn, m_dbi, &key, &val, 0);
	if (rc)
    {
        LOG(ERROR) << "could not set dirty bit: " << mdb_strerror(rc);
		mdb_txn_abort(txn);
        return false;
    }

    size_t sz = sizeof(uint64_t)
              + pack_size(bind_to)
              + pack_size(coordinator);
    std::auto_ptr<e::buffer> state(e::buffer::create(sz));
    *state << us << bind_to << coordinator;
	MVS(key, "state");
	val.mv_data = state->data();
	val.mv_size = state->size();
	rc = mdb_put(txn, m_dbi, &key, &val, 0);
	if (rc == MDB_SUCCESS)
	{
		rc = mdb_txn_commit(txn);
		if (rc == MDB_SUCCESS)
			return true;
		txn = NULL;
	}

	if (txn)
		mdb_txn_abort(txn);

    LOG(ERROR) << "could not save state: " << mdb_strerror(rc);
    return false;
}

bool
datalayer :: clear_dirty()
{
	MDB_txn *txn;
	MDB_val key;
	int rc;

	MVS(key, "dirty");

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
		LOG(ERROR) << "could not open txn: " << mdb_strerror(rc);
		return false;
	}
	rc = mdb_del(txn, m_dbi, &key, 0);
	if (rc == MDB_NOTFOUND)
	{
		mdb_txn_abort(txn);
		return true;
	}
	if (rc == MDB_SUCCESS)
	{
		rc = mdb_txn_commit(txn);
		if (rc == MDB_SUCCESS)
			return true;
		txn = NULL;
	}
	if (txn)
		mdb_txn_abort(txn);

    LOG(ERROR) << "could not clear dirty bit: " << mdb_strerror(rc);
    return false;
}

void
datalayer :: pause()
{
    po6::threads::mutex::hold hold(&m_block_cleaner);
    assert(!m_need_pause);
    m_need_pause = true;
}

void
datalayer :: unpause()
{
    po6::threads::mutex::hold hold(&m_block_cleaner);
    assert(m_need_pause);
    m_wakeup_cleaner.broadcast();
    m_need_pause = false;
    m_need_cleaning = true;
}

void
datalayer :: reconfigure(const configuration&,
                         const configuration& new_config,
                         const server_id& us)
{
    {
        po6::threads::mutex::hold hold(&m_block_cleaner);
        assert(m_need_pause);

        while (!m_paused)
        {
            m_wakeup_reconfigurer.wait();
        }
    }

    std::vector<capture> captures;
    new_config.captures(&captures);
    std::vector<region_id> regions;
    regions.reserve(captures.size());

    for (size_t i = 0; i < captures.size(); ++i)
    {
        if (new_config.get_virtual(captures[i].rid, us) != virtual_server_id())
        {
            regions.push_back(captures[i].rid);
        }
    }

    std::sort(regions.begin(), regions.end());
    m_counters.adopt(regions);
}

bool
datalayer :: get_property(const e::slice& property,
                          std::string* value)
{
	return false;
}

uint64_t
datalayer :: approximate_size()
{
    uint64_t ret;
	MDB_stat st;
	mdb_env_stat(m_db, &st);
	ret = (st.ms_branch_pages + st.ms_leaf_pages + st.ms_overflow_pages) * st.ms_psize;

    return ret;
}

datalayer::returncode
datalayer :: get(const region_id& ri,
                 const e::slice& key,
                 std::vector<e::slice>* value,
                 uint64_t* version,
                 reference* ref)
{
	int rc;
	MDB_val k, val;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;

    // create the encoded key
    DB_SLICE lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

    // perform the read
	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &ref->m_rtxn);
	if (rc)
	{
		return handle_error(rc);
	}
	MVSL(k,lkey);
	rc = mdb_get(ref->m_rtxn, m_dbi, &k, &val);
	if (rc == MDB_SUCCESS)
    {
		ref->m_slice = e::slice(val.mv_data, val.mv_size);
        return decode_value(ref->m_slice, value, version);
    }
    else if (rc == MDB_NOTFOUND)
    {
        return NOT_FOUND;
    }
    else
    {
        return handle_error(rc);
    }
}

datalayer::returncode
datalayer :: del(const region_id& ri,
                 const region_id& reg_id,
                 uint64_t seq_id,
                 const e::slice& key,
                 const std::vector<e::slice>& old_value)
{
    MDB_txn *updates;
	MDB_val k, val;
	int rc;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;

    // create the encoded key
    DB_SLICE lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

	rc = mdb_txn_begin(m_db, NULL, 0, &updates);
	if (rc)
	{
		return handle_error(rc);
	}

    // delete the actual object
	MVSL(k,lkey);
	rc = mdb_del(updates, m_dbi, &k, 0);
	if (rc)
	{
		mdb_txn_abort(updates);
		return handle_error(rc);
	}

    // delete the index entries
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));
    create_index_changes(sc, sub, ri, key, &old_value, NULL, updates);

    // Mark acked as part of this batch write
    if (seq_id != 0)
    {
        char abacking[ACKED_BUF_SIZE];
        seq_id = UINT64_MAX - seq_id;
        encode_acked(ri, reg_id, seq_id, abacking);
		MVBF(k,abacking);
		MVS(val, "");
		rc = mdb_put(updates, m_dbi, &k, &val, 0);
		if (rc) {
			mdb_txn_abort(updates);
			return handle_error(rc);
		}
    }

    uint64_t count;

    // If this is a captured region, then we must log this transfer
    if (m_counters.lookup(ri, &count))
    {
        char tbacking[TRANSFER_BUF_SIZE];
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
		MVBF(k,tbacking);
        DB_SLICE tval;
        encode_transfer(cid, count, tbacking);
        encode_key_value(key, NULL, 0, &scratch, &tval);
		MVSL(val,tval);
		rc = mdb_put(updates, m_dbi, &k, &val, 0);
		if (rc) {
			mdb_txn_abort(updates);
			return handle_error(rc);
		}
    }

    // Perform the write
	rc = mdb_txn_commit(updates);

    if (rc == MDB_SUCCESS)
    {
        return SUCCESS;
    }
    else if (rc == MDB_NOTFOUND)
    {
        return NOT_FOUND;
    }
    else
    {
        return handle_error(rc);
    }
}

datalayer::returncode
datalayer :: put(const region_id& ri,
                 const region_id& reg_id,
                 uint64_t seq_id,
                 const e::slice& key,
                 const std::vector<e::slice>& new_value,
                 uint64_t version)
{
    MDB_txn *updates;
	MDB_val k, val;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch1;
    std::vector<char> scratch2;
	int rc;

	rc = mdb_txn_begin(m_db, NULL, 0, &updates);
	if (rc)
	{
		return handle_error(rc);
	}

    // create the encoded key
    DB_SLICE lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch1, &lkey);

    // create the encoded value
    DB_SLICE lval;
    encode_value(new_value, version, &scratch2, &lval);

    // put the actual object
	MVSL(k,lkey);
	MVSL(val,lval);
	rc = mdb_put(updates, m_dbi, &k, &val, 0);
	if (rc)
	{
		mdb_txn_abort(updates);
		return handle_error(rc);
	}

    // put the index entries
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));
    create_index_changes(sc, sub, ri, key, NULL, &new_value, updates);

    // Mark acked as part of this batch write
    if (seq_id != 0)
    {
        char abacking[ACKED_BUF_SIZE];
        seq_id = UINT64_MAX - seq_id;
		MVBF(k,abacking);
		MVS(val, "");
        encode_acked(ri, reg_id, seq_id, abacking);
        rc = mdb_put(updates, m_dbi, &k, &val, 0);
		if (rc)
		{
			mdb_txn_abort(updates);
			return handle_error(rc);
		}
    }

    uint64_t count;

    // If this is a captured region, then we must log this transfer
    if (m_counters.lookup(ri, &count))
    {
        char tbacking[TRANSFER_BUF_SIZE];
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
        DB_SLICE tval;
		MVBF(k,tbacking);
        encode_transfer(cid, count, tbacking);
        encode_key_value(key, &new_value, version, &scratch1, &tval);
		MVSL(val,tval);
        rc = mdb_put(updates, m_dbi, &k, &val, 0);
		if (rc)
		{
			mdb_txn_abort(updates);
			return handle_error(rc);
		}
    }

    // Perform the write
	rc = mdb_txn_commit(updates);

    if (rc == MDB_SUCCESS)
    {
        return SUCCESS;
    }
    else
    {
        return handle_error(rc);
    }
}

datalayer::returncode
datalayer :: overput(const region_id& ri,
                     const region_id& reg_id,
                     uint64_t seq_id,
                     const e::slice& key,
                     const std::vector<e::slice>& old_value,
                     const std::vector<e::slice>& new_value,
                     uint64_t version)
{
    MDB_txn *updates;
	MDB_val k, val;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch1;
    std::vector<char> scratch2;
	int rc;

	rc = mdb_txn_begin(m_db, NULL, 0, &updates);
	if (rc)
	{
		return handle_error(rc);
	}

    // create the encoded key
    DB_SLICE lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch1, &lkey);

    // create the encoded value
    DB_SLICE lval;
    encode_value(new_value, version, &scratch2, &lval);

    // put the actual object
	MVSL(k,lkey);
	MVSL(val,lval);
	rc = mdb_put(updates, m_dbi, &k, &val, 0);
	if (rc)
	{
		mdb_txn_abort(updates);
		return handle_error(rc);
	}

    // put the index entries
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));
    create_index_changes(sc, sub, ri, key, &old_value, &new_value, updates);

    // Mark acked as part of this batch write
    if (seq_id != 0)
    {
        char abacking[ACKED_BUF_SIZE];
        seq_id = UINT64_MAX - seq_id;
        encode_acked(ri, reg_id, seq_id, abacking);
		MVBF(k,abacking);
		MVS(val, "");
        rc = mdb_put(updates, m_dbi, &k, &val, 0);
		if (rc)
		{
			mdb_txn_abort(updates);
			return handle_error(rc);
		}
    }

    uint64_t count;

    // If this is a captured region, then we must log this transfer
    if (m_counters.lookup(ri, &count))
    {
        char tbacking[TRANSFER_BUF_SIZE];
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
		MVBF(k, tbacking);
        DB_SLICE tval;
        encode_transfer(cid, count, tbacking);
        encode_key_value(key, &new_value, version, &scratch1, &tval);
		MVSL(val,tval);
        rc = mdb_put(updates, m_dbi, &k, &val, 0);
		if (rc)
		{
			mdb_txn_abort(updates);
			return handle_error(rc);
		}
    }

    // Perform the write
	rc = mdb_txn_commit(updates);

    if (rc == MDB_SUCCESS)
    {
        return SUCCESS;
    }
    else
    {
        return handle_error(rc);
    }
}

datalayer::returncode
datalayer :: uncertain_del(const region_id& ri,
                           const e::slice& key)
{
    MDB_txn *txn;
	MDB_val k, val;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;
	int rc;

	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &txn);
	if (rc)
	{
		return handle_error(rc);
	}

    // create the encoded key
    DB_SLICE lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

    // perform the read
	MVSL(k,lkey);
	rc = mdb_get(txn, m_dbi, &k, &val);

    if (rc == MDB_SUCCESS)
    {
        std::vector<e::slice> old_value;
        uint64_t old_version;
        returncode rc = decode_value(e::slice(val.mv_data, val.mv_size),
                                     &old_value, &old_version);

        if (rc != SUCCESS)
        {
			mdb_txn_abort(txn);
            return rc;
        }

        if (old_value.size() + 1 != sc.attrs_sz)
        {
			mdb_txn_abort(txn);
            return BAD_ENCODING;
        }

        rc =  del(ri, region_id(), 0, key, old_value);
		mdb_txn_abort(txn);
		return rc;
    }
	mdb_txn_abort(txn);
    if (rc == MDB_NOTFOUND)
    {
        return SUCCESS;
    }
    else
    {
        return handle_error(rc);
    }
}

datalayer::returncode
datalayer :: uncertain_put(const region_id& ri,
                           const e::slice& key,
                           const std::vector<e::slice>& new_value,
                           uint64_t version)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;
	MDB_txn *txn;
	MDB_val k, val;
	int rc;

	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &txn);
	if (rc)
	{
		return handle_error(rc);
	}

    // create the encoded key
    DB_SLICE lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

    // perform the read
	MVSL(k,lkey);
	rc = mdb_get(txn, m_dbi, &k, &val);

    if (rc == MDB_SUCCESS)
    {
        std::vector<e::slice> old_value;
        uint64_t old_version;
        returncode rc = decode_value(e::slice(val.mv_data, val.mv_size),
                                     &old_value, &old_version);

        if (rc != SUCCESS)
        {
			mdb_txn_abort(txn);
            return rc;
        }

        if (old_value.size() + 1 != sc.attrs_sz)
        {
			mdb_txn_abort(txn);
            return BAD_ENCODING;
        }

        rc = overput(ri, region_id(), 0, key, old_value, new_value, version);
		mdb_txn_abort(txn);
		return rc;
    }
	mdb_txn_abort(txn);
    if (rc == MDB_NOTFOUND)
    {
        return put(ri, region_id(), 0, key, new_value, version);
    }
    else
    {
        return handle_error(rc);
    }
}

datalayer::returncode
datalayer :: get_transfer(const region_id& ri,
                          uint64_t seq_no,
                          bool* has_value,
                          e::slice* key,
                          std::vector<e::slice>* value,
                          uint64_t* version,
                          reference* ref)
{
	MDB_val k,val;
	int rc;
    char tbacking[TRANSFER_BUF_SIZE];
    capture_id cid = m_daemon->m_config.capture_for(ri);
    assert(cid != capture_id());
	MVBF(k,tbacking);
	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &ref->m_rtxn);
	if (rc)
	{
		return handle_error(rc);
	}
    encode_transfer(cid, seq_no, tbacking);
	rc = mdb_get(ref->m_rtxn, m_dbi, &k, &val);

    if (rc == MDB_SUCCESS)
    {
        ref->m_slice = e::slice(val.mv_data, val.mv_size);
        return decode_key_value(ref->m_slice, has_value, key, value, version);
    }
    else if (rc == MDB_NOTFOUND)
    {
        return NOT_FOUND;
    }
    else
    {
        return handle_error(rc);
    }
}

bool
datalayer :: check_acked(const region_id& ri,
                         const region_id& reg_id,
                         uint64_t seq_id)
{
	MDB_txn *txn;
    // make it so that increasing seq_ids are ordered in reverse in the KVS
    seq_id = UINT64_MAX - seq_id;
	int rc;
    char abacking[ACKED_BUF_SIZE];
	MDB_val k;
    encode_acked(ri, reg_id, seq_id, abacking);
	MVBF(k,abacking);

	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &txn);
	if (rc)
	{
		return false;
	}
	rc = mdb_get(txn, m_dbi, &k, NULL);
	mdb_txn_abort(txn);
    if (rc == MDB_SUCCESS)
    {
        return true;
    }
    else if (rc == MDB_NOTFOUND)
    {
        return false;
    }
    else
    {
        LOG(ERROR) << "DB error at region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << mdb_strerror(rc);
        return false;
    }
}

void
datalayer :: mark_acked(const region_id& ri,
                        const region_id& reg_id,
                        uint64_t seq_id)
{
	MDB_txn *txn;
	MDB_val k, val;
    // make it so that increasing seq_ids are ordered in reverse in the KVS
    seq_id = UINT64_MAX - seq_id;
	int rc;
    char abacking[ACKED_BUF_SIZE];
    encode_acked(ri, reg_id, seq_id, abacking);
	MVBF(k,abacking);
	MVS(val, "");

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
		return;
	}
	rc = mdb_put(txn, m_dbi, &k, &val, 0);
	if (rc == MDB_SUCCESS)
		rc = mdb_txn_commit(txn);
	else
		mdb_txn_abort(txn);

    if (rc == MDB_SUCCESS)
    {
        // Yay!
    }
    else if (rc == MDB_NOTFOUND)
    {
        LOG(ERROR) << "mark_acked returned NOT_FOUND at the disk layer: region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << mdb_strerror(rc);
    }
    else
    {
        LOG(ERROR) << "DB error at region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << mdb_strerror(rc);
    }
}

void
datalayer :: max_seq_id(const region_id& reg_id,
                        uint64_t* seq_id)
{
	MDB_txn *txn;
	MDB_cursor *mc;
	MDB_val k;
    char abacking[ACKED_BUF_SIZE];
	int rc;

	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &txn);
	if (rc)
	{
		return;
	}
	rc = mdb_cursor_open(txn, m_dbi, &mc);
	if (rc)
	{
		mdb_txn_abort(txn);
		return;
	}
    encode_acked(reg_id, reg_id, 0, abacking);
	MVBF(k,abacking);
	rc = mdb_cursor_get(mc, &k, NULL, MDB_SET_RANGE);
	mdb_cursor_close(mc);

    if (rc == MDB_NOTFOUND)
    {
        *seq_id = 0;
		mdb_txn_abort(txn);
        return;
    }

    region_id tmp_ri;
    region_id tmp_reg_id;
    uint64_t tmp_seq_id;
    rc = decode_acked(e::slice(k.mv_data, k.mv_size),
                                            &tmp_ri, &tmp_reg_id, &tmp_seq_id);
	mdb_txn_abort(txn);

    if (rc != SUCCESS || tmp_ri != reg_id || tmp_reg_id != reg_id)
    {
        *seq_id = 0;
        return;
    }

    *seq_id = UINT64_MAX - tmp_seq_id;
}

void
datalayer :: clear_acked(const region_id& reg_id,
                         uint64_t seq_id)
{
	MDB_txn *txn;
	MDB_cursor *mc;
	MDB_val k, upper;
	int rc;
    char abacking[ACKED_BUF_SIZE];

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
		return;
	}
	rc = mdb_cursor_open(txn, m_dbi, &mc);
	if (rc)
	{
		mdb_txn_abort(txn);
		return;
	}
    encode_acked(region_id(0), reg_id, 0, abacking);
	MVBF(k,abacking);
	rc = mdb_cursor_get(mc, &k, NULL, MDB_SET_RANGE);
    encode_acked(region_id(0), region_id(reg_id.get() + 1), 0, abacking);
	MVBF(upper,abacking);

	while (rc == MDB_SUCCESS &&
		mdb_cmp(txn, m_dbi, &k, &upper) < 0)
    {
        region_id tmp_ri;
        region_id tmp_reg_id;
        uint64_t tmp_seq_id;
        rc = decode_acked(e::slice(k.mv_data, k.mv_size),
                                                &tmp_ri, &tmp_reg_id, &tmp_seq_id);
        tmp_seq_id = UINT64_MAX - tmp_seq_id;

        if (rc == SUCCESS &&
            tmp_reg_id == reg_id &&
            tmp_seq_id < seq_id)
        {
			rc = mdb_cursor_del(mc, 0);

            if (rc == MDB_SUCCESS)
            {
                // WOOT!
				// delete automatically points cursor to next item
				rc = mdb_cursor_get(mc, &k, NULL, MDB_GET_CURRENT);
            }
            else
            {
                LOG(ERROR) << "DB error: could not delete "
                           << reg_id << " " << seq_id << ": desc=" << mdb_strerror(rc);
            }
        } else 
		{
			rc = mdb_cursor_get(mc, &k, NULL, MDB_NEXT);
		}
    }
	mdb_cursor_close(mc);
	rc = mdb_txn_commit(txn);
	if (rc)
	{
		LOG(ERROR) << "DB error: could not commit: " << mdb_strerror(rc);
	}
}

void
datalayer :: request_wipe(const capture_id& cid)
{
    po6::threads::mutex::hold hold(&m_block_cleaner);
    m_state_transfer_captures.insert(cid);
    m_wakeup_cleaner.broadcast();
}

datalayer::snapshot
datalayer :: make_snapshot()
{
	int rc;
	MDB_txn *ret = NULL;
	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &ret);
	if (rc)
	{
		LOG(ERROR) << "DB error: could not make rdonly txn: " << mdb_strerror(rc);
	}
    return ret;
}

datalayer::iterator*
datalayer :: make_region_iterator(snapshot snap,
                                  const region_id& ri,
                                  returncode* error)
{
	MDB_cursor *iter = NULL;
    *error = datalayer::SUCCESS;
    const size_t backing_sz = sizeof(uint8_t) + sizeof(uint64_t);
    char backing[backing_sz];
    char* ptr = backing;
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(ri.get(), ptr);

	mdb_cursor_open(snap, m_dbi, &iter);
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    return new region_iterator(iter, ri, index_info::lookup(sc.attrs[0].type));
}


datalayer::iterator*
datalayer :: make_search_iterator(snapshot snap,
                                  const region_id& ri,
                                  const std::vector<attribute_check>& checks,
                                  std::ostringstream* ostr)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<e::intrusive_ptr<index_iterator> > iterators;

    // pull a set of range queries from checks
    std::vector<range> ranges;
    range_searches(checks, &ranges);
    index_info* ki = index_info::lookup(sc.attrs[0].type);
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));

    // for each range query, construct an iterator
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        if (ranges[i].invalid)
        {
            if (ostr) *ostr << "encountered invalid range; returning no results\n";
            return new dummy_iterator();
        }

        assert(ranges[i].attr < sc.attrs_sz);
        assert(ranges[i].type == sc.attrs[ranges[i].attr].type);

        if (ostr) *ostr << "considering attr " << ranges[i].attr << " Range("
                        << ranges[i].start.hex() << ", " << ranges[i].end.hex() << " " << ranges[i].type << " "
                        << (ranges[i].has_start ? "[" : "<") << "-" << (ranges[i].has_end ? "]" : ">")
                        << " " << (ranges[i].invalid ? "invalid" : "valid") << "\n";

        if (!sub.indexed(ranges[i].attr))
        {
            continue;
        }

        index_info* ii = index_info::lookup(ranges[i].type);

        if (ii)
        {
            e::intrusive_ptr<index_iterator> it = ii->iterator_from_range(snap, ri, ranges[i], ki);

            if (it)
            {
                iterators.push_back(it);
            }
        }
    }

    // for everything that is not a range query, construct an iterator
    for (size_t i = 0; i < checks.size(); ++i)
    {
        if (checks[i].predicate == HYPERPREDICATE_EQUALS ||
            checks[i].predicate == HYPERPREDICATE_LESS_EQUAL ||
            checks[i].predicate == HYPERPREDICATE_GREATER_EQUAL)
        {
            continue;
        }

        if (!sub.indexed(checks[i].attr))
        {
            continue;
        }

        index_info* ii = index_info::lookup(sc.attrs[checks[i].attr].type);

        if (ii)
        {
            e::intrusive_ptr<index_iterator> it = ii->iterator_from_check(snap, ri, checks[i], ki);

            if (it)
            {
                iterators.push_back(it);
            }
        }
    }

    // figure out the cost of accessing all objects
    e::intrusive_ptr<index_iterator> full_scan;
    range scan;
    scan.attr = 0;
    scan.type = sc.attrs[0].type;
    scan.has_start = false;
    scan.has_end = false;
    scan.invalid = false;
    full_scan = ki->iterator_from_range(snap, ri, scan, ki);
    if (ostr) *ostr << "accessing all objects has cost " << full_scan->cost() << "\n";

    // figure out the cost of each iterator
    // we do this here and not below so that iterators can cache the size and we
    // don't ping-pong between HyperDex and LevelDB.
    for (size_t i = 0; i < iterators.size(); ++i)
    {
        uint64_t iterator_cost = iterators[i]->cost();
        if (ostr) *ostr << "iterator " << *iterators[i] << " has cost " << iterator_cost << "\n";
    }

    std::vector<e::intrusive_ptr<index_iterator> > sorted;
    std::vector<e::intrusive_ptr<index_iterator> > unsorted;

    for (size_t i = 0; i < iterators.size(); ++i)
    {
        if (iterators[i]->sorted())
        {
            sorted.push_back(iterators[i]);
        }
        else
        {
            unsorted.push_back(iterators[i]);
        }
    }

    e::intrusive_ptr<index_iterator> best;

    if (!sorted.empty())
    {
        best = new intersect_iterator(snap, sorted);
    }

    if (!best || best->cost() * 4 > full_scan->cost())
    {
        best = full_scan;
    }

    // just pick one; do something smart later
    if (!unsorted.empty() && !best)
    {
        best = unsorted[0];
    }

    assert(best);
    if (ostr) *ostr << "choosing to use " << *best << "\n";
    return new search_iterator(this, ri, best, ostr, &checks);
}

datalayer::returncode
datalayer :: get_from_iterator(const region_id& ri,
                               iterator* iter,
                               e::slice* key,
                               std::vector<e::slice>* value,
                               uint64_t* version,
                               reference* ref)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;
	MDB_val k, val;
	int rc;

    // create the encoded key
    DB_SLICE lkey;
    encode_key(ri, sc.attrs[0].type, iter->key(), &scratch, &lkey);

    // perform the read
	MVSL(k,lkey);
	rc = mdb_get(iter->snap(), m_dbi, &k, &val);

    if (rc == MDB_SUCCESS)
    {
		ref->m_backing = std::string((const char *)val.mv_data, val.mv_size);
        ref->m_backing += std::string(reinterpret_cast<const char*>(iter->key().data()), iter->key().size());
        *key = e::slice(ref->m_backing.data()
                        + ref->m_backing.size()
                        - iter->key().size(),
                        iter->key().size());
        e::slice v(ref->m_backing.data(), ref->m_backing.size() - iter->key().size());
        return decode_value(v, value, version);
    }
    else if (rc == MDB_NOTFOUND)
    {
        return NOT_FOUND;
    }
    else
    {
        return handle_error(rc);
    }
}

void
datalayer :: cleaner()
{
    LOG(INFO) << "cleanup thread started";
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return;
    }

    while (true)
    {
        std::set<capture_id> state_transfer_captures;

        {
            po6::threads::mutex::hold hold(&m_block_cleaner);

            while ((!m_need_cleaning &&
                    m_state_transfer_captures.empty() &&
                    !m_shutdown) || m_need_pause)
            {
                m_paused = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_cleaner.wait();
                m_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            m_state_transfer_captures.swap(state_transfer_captures);
            m_need_cleaning = false;
        }

		MDB_txn *txn;
		MDB_cursor *mc;
		MDB_val k;
		int rc;

		rc = mdb_txn_begin(m_db, NULL, 0, &txn);
		rc = mdb_cursor_open(txn, m_dbi, &mc);
		MVS(k, "t");
		rc = mdb_cursor_get(mc, &k, NULL, MDB_SET_RANGE);
        capture_id cached_cid;

        while (rc == MDB_SUCCESS)
        {
            uint8_t prefix;
            uint64_t cid;
            uint64_t seq_no;
            e::unpacker up((const char *)k.mv_data, k.mv_size);
            up = up >> prefix >> cid >> seq_no;

            if (up.error() || prefix != 't')
            {
                break;
            }

            if (cid == cached_cid.get())
            {
				rc = mdb_cursor_del(mc, 0);

                if (rc == MDB_SUCCESS)
                {
                    // pass
                }
                else
                {
                    LOG(ERROR) << "DB could not cleanup old transfers:"
                               << " desc=" << mdb_strerror(rc);
                }

                rc = mdb_cursor_get(mc, &k, NULL, MDB_GET_CURRENT);
                continue;
            }

            m_daemon->m_stm.report_wiped(cached_cid);

            if (!m_daemon->m_config.is_captured_region(capture_id(cid)))
            {
                cached_cid = capture_id(cid);
                continue;
            }

            if (state_transfer_captures.find(capture_id(cid)) != state_transfer_captures.end())
            {
                cached_cid = capture_id(cid);
                state_transfer_captures.erase(cached_cid);
                continue;
            }

            char tbacking[TRANSFER_BUF_SIZE];
            DB_SLICE slice(tbacking, TRANSFER_BUF_SIZE);
            encode_transfer(capture_id(cid + 1), 0, tbacking);
			MVBF(k, tbacking);
			rc = mdb_cursor_get(mc, &k, NULL, MDB_SET_RANGE);
        }
		mdb_cursor_close(mc);
		rc = mdb_txn_commit(txn);
		if (rc)
		{
            LOG(ERROR) << "DB could not cleanup old transfers:"
                       << " desc=" << mdb_strerror(rc);
		}

        while (!state_transfer_captures.empty())
        {
            m_daemon->m_stm.report_wiped(*state_transfer_captures.begin());
            state_transfer_captures.erase(state_transfer_captures.begin());
        }
    }

    LOG(INFO) << "cleanup thread shutting down";
}

void
datalayer :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold hold(&m_block_cleaner);
        m_wakeup_cleaner.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_cleaner.join();
    }
}

datalayer::returncode
datalayer :: handle_error(int rc)
{
    if (rc == MDB_CORRUPTED)
    {
        LOG(ERROR) << "corruption at the disk layer: " << mdb_strerror(rc);
        return CORRUPTION;
    }
    else if (rc == MDB_PANIC)
    {
        LOG(ERROR) << "IO error at the disk layer: " << mdb_strerror(rc);
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "DB returned an error that we don't know how to handle " << mdb_strerror(rc);
        return DB_ERROR;
    }
}

datalayer :: reference :: reference()
    : m_backing(), m_rtxn()
{
}

datalayer :: reference :: ~reference() throw ()
{
	mdb_txn_abort(m_rtxn);
}

void
datalayer :: reference :: swap(reference* ref)
{
    m_backing.swap(ref->m_backing);
}

void
datalayer :: reference :: persist()
{
	m_backing.assign((const char *)m_slice.data(), m_slice.size());
	mdb_txn_abort(m_rtxn);
	m_rtxn = NULL;
}

std::ostream&
hyperdex :: operator << (std::ostream& lhs, datalayer::returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(datalayer::SUCCESS);
        STRINGIFY(datalayer::NOT_FOUND);
        STRINGIFY(datalayer::BAD_ENCODING);
        STRINGIFY(datalayer::CORRUPTION);
        STRINGIFY(datalayer::IO_ERROR);
        STRINGIFY(datalayer::DB_ERROR);
        default:
            lhs << "unknown returncode";
    }

    return lhs;
}
