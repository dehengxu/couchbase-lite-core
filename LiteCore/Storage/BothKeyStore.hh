//
// BothKeyStore.hh
//
// Copyright © 2019 Couchbase. All rights reserved.
//

#pragma once
#include "KeyStore.hh"
#include "Query.hh"
#include "Error.hh"

namespace litecore {

    /** A fake KeyStore that combines a real KeyStore for live documents and another for tombstones,
        and makes them appear to be a single store.
        All live documents are in the live store; all deleted documents are in the dead store.
        Sequence numbers are shared across both stores. */
    class BothKeyStore : public KeyStore {
    public:
        BothKeyStore(KeyStore *liveStore NONNULL, KeyStore *deadStore NONNULL);


        void shareSequencesWith(KeyStore&) override         {Assert(false);}

        virtual uint64_t recordCount(bool includeDeleted =false) const override;
        
        virtual sequence_t lastSequence() const override    {return _liveStore->lastSequence();}
        virtual uint64_t purgeCount() const override        {return _liveStore->purgeCount();}


        //// CRUD:

        virtual bool read(Record &rec, ContentOption option) const override {
            return _liveStore->read(rec, option) || _deadStore->read(rec, option);
        }


        virtual Record get(sequence_t seq) const override {
            auto rec = _liveStore->get(seq);
            return rec.exists() ? rec :_deadStore->get(seq);
        }


        virtual sequence_t set(slice key, slice version, slice value,
                               DocumentFlags flags,
                               Transaction &t,
                               const sequence_t *replacingSequence =nullptr,
                               bool newSequence =true) override;


        virtual bool del(slice key, Transaction &t, sequence_t replacingSequence) override {
            // Always delete from both stores, for safety's sake.
            bool a = _liveStore->del(key, t, replacingSequence);
            bool b = _deadStore->del(key, t, replacingSequence);
            return a || b;
        }


        virtual bool setDocumentFlag(slice key, sequence_t seq,
                                     DocumentFlags flags, Transaction &t) override
        {
            Assert(!(flags & DocumentFlags::kDeleted));     // this method isn't used for deleting
            return _liveStore->setDocumentFlag(key, seq, flags, t)
                || _deadStore->setDocumentFlag(key, seq, flags, t);
        }


        virtual void transactionWillEnd(bool commit) override {
            _liveStore->transactionWillEnd(commit);
            _deadStore->transactionWillEnd(commit);
        }


        //// EXPIRATION:

        virtual bool setExpiration(slice key, expiration_t exp) override {
            return _liveStore->setExpiration(key, exp) || _deadStore->setExpiration(key, exp);
        }


        virtual expiration_t getExpiration(slice key) override {
            return std::max(_liveStore->getExpiration(key), _deadStore->getExpiration(key));
        }

        virtual expiration_t nextExpiration() override;


        virtual unsigned expireRecords(ExpirationCallback callback =nullptr) override {
            return _liveStore->expireRecords(callback) + _deadStore->expireRecords(callback);
        }


        //// QUERIES & INDEXES:

        virtual Retained<Query> compileQuery(slice expr, QueryLanguage language) override {
            return _liveStore->compileQuery(expr, language);
        }

        virtual std::vector<alloc_slice> withDocBodies(const std::vector<slice> &docIDs,
                                                       WithDocBodyCallback callback) override;

        virtual bool supportsIndexes(IndexSpec::Type type) const override {
            return _liveStore->supportsIndexes(type);
        }

        virtual bool createIndex(const IndexSpec &spec) override {
            return _liveStore->createIndex(spec);
        }


        virtual void deleteIndex(slice name) override {
            _liveStore->deleteIndex(name);
        }


        virtual std::vector<IndexSpec> getIndexes() const override {
            return _liveStore->getIndexes();
        }


    protected:
        virtual void reopen() override              {_liveStore->reopen(); _deadStore->reopen();}
        virtual void close() override               {_liveStore->close(); _deadStore->close();}
        virtual RecordEnumerator::Impl* newEnumeratorImpl(bool bySequence,
                                                          sequence_t since,
                                                          RecordEnumerator::Options) override;
    private:
        std::unique_ptr<KeyStore> _liveStore;
        std::unique_ptr<KeyStore> _deadStore;
    };


}
