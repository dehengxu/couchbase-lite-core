//
// BothKeyStore.cc
//
// Copyright © 2019 Couchbase. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "BothKeyStore.hh"
#include "RecordEnumerator.hh"
#include <memory>

namespace litecore {
    using namespace std;

    BothKeyStore::BothKeyStore(KeyStore *liveStore, KeyStore *deadStore)
    :KeyStore(liveStore->dataFile(), liveStore->name(), liveStore->capabilities())
    ,_liveStore(liveStore)
    ,_deadStore(deadStore)
    {
        deadStore->shareSequencesWith(*liveStore);
    }


    sequence_t BothKeyStore::set(slice key, slice version, slice value,
                                 DocumentFlags flags,
                                 Transaction &t,
                                 const sequence_t *replacingSequence,
                                 bool newSequence)
    {
        bool deleting = (flags & DocumentFlags::kDeleted);
        auto target = (deleting ? _deadStore : _liveStore).get();   // the store to update
        auto other  = (deleting ? _liveStore : _deadStore).get();

        if (replacingSequence && *replacingSequence == 0) {
            // Request should succeed only if doc _doesn't_ exist yet, so check other KeyStore:
            bool exists = false;
            other->get(key, kMetaOnly, [&](const Record &rec) {
                exists = rec.exists();
            });
            if (exists)
                return 0;
        }

        // Forward the 'set' to the target store:
        auto seq = target->set(key, version, value, flags, t, replacingSequence, newSequence);

        if (seq > 0 && !replacingSequence) {
            // Have to manually nuke any older rev from the other store:
            // OPT: Try to avoid this!
            other->del(key, t);
        } else if (seq == 0 && replacingSequence && *replacingSequence > 0) {
            // Wrong sequence. Maybe record is currently in the other KeyStore; if so, delete it
            Assert(newSequence);
            if (other->del(key, t, *replacingSequence))
                seq = target->set(key, version, value, flags, t, nullptr, newSequence);
        }
        return seq;
    }


    std::vector<alloc_slice> BothKeyStore::withDocBodies(const std::vector<slice> &docIDs,
                                                         WithDocBodyCallback callback)
    {
        // First, delegate to the live store:
        size_t nDocs = docIDs.size();
        auto result = _liveStore->withDocBodies(docIDs, callback);

        // Collect the docIDs that weren't found in the live store:
        std::vector<slice> recheckDocs;
        std::vector<size_t> recheckIndexes(nDocs);
        size_t nRecheck = 0;
        for (size_t i = 0; i < nDocs; ++i) {
            if (!result[i]) {
                recheckDocs.push_back(docIDs[i]);
                recheckIndexes[nRecheck++] = i;
            }
        }

        // Retry those docIDs in the dead store and add any results:
        if (nRecheck > 0) {
            auto dead = _deadStore->withDocBodies(recheckDocs, callback);
            for (size_t i = 0; i < nRecheck; ++i) {
                if (dead[i])
                    result[recheckIndexes[i]] = dead[i];
            }
        }

        return result;
    }


    expiration_t BothKeyStore::nextExpiration() {
        auto lx = _liveStore->nextExpiration();
        auto dx = _deadStore->nextExpiration();
        if (lx > 0 && dx > 0)
            return std::min(lx, dx);        // choose the earliest time
        else
            return std::max(lx, dx);        // choose the nonzero time
    }


#pragma mark - ENUMERATOR:


    // Enumerator implementation for BothKeyStore. It enumerates both KeyStores in parallel,
    // always returning the lowest-sorting record (basically a merge-sort.)
    class BothEnumeratorImpl : public RecordEnumerator::Impl {
    public:
        BothEnumeratorImpl(bool bySequence,
                           sequence_t since,
                           RecordEnumerator::Options options,
                           KeyStore *liveStore, KeyStore *deadStore)
        :_liveImpl(liveStore->newEnumeratorImpl(bySequence, since, options))
        ,_deadImpl(deadStore->newEnumeratorImpl(bySequence, since, options))
        ,_bySequence(bySequence)
        { }

        virtual bool next() override {
            // Advance the enumerator whose value was used last:
            if (_current == nullptr || _current == _liveImpl.get()) {
                if (!_liveImpl->next())
                    _liveImpl.reset();
            }
            if (_current == nullptr || _current == _deadImpl.get()) {
                if (!_deadImpl->next())
                    _deadImpl.reset();
            }

            // Pick the enumerator with the lowest key/sequence to be used next:
            bool useLive;
            if (_liveImpl && _deadImpl) {
                if (_bySequence)
                    useLive = _liveImpl->sequence() < _deadImpl->sequence();
                else
                    useLive = _liveImpl->key() < _deadImpl->key();
            } else if (_liveImpl || _deadImpl) {
                useLive = _liveImpl != nullptr;
            } else {
                _current = nullptr;
                return false;
            }

            _current = (useLive ? _liveImpl : _deadImpl).get();
            return true;
        }

        virtual bool read(Record &record) const override    {return _current->read(record);}
        virtual slice key() const override                  {return _current->key();}
        virtual sequence_t sequence() const override        {return _current->sequence();}

    private:
        unique_ptr<RecordEnumerator::Impl> _liveImpl, _deadImpl;    // Real enumerators
        RecordEnumerator::Impl* _current {nullptr};                 // Enumerator w/lowest key
        bool _bySequence;                                           // Sorting by sequence?
    };


    RecordEnumerator::Impl* BothKeyStore::newEnumeratorImpl(bool bySequence,
                                                            sequence_t since,
                                                            RecordEnumerator::Options options)
    {
        if (options.includeDeleted) {
            return new BothEnumeratorImpl(bySequence, since, options,
                                          _liveStore.get(), _deadStore.get());
        } else {
            return _liveStore->newEnumeratorImpl(bySequence, since, options);
        }
    }


}
