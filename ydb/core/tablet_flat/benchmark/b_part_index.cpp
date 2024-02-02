#include <benchmark/benchmark.h>

#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tablet_flat/flat_part_charge_range.h>
#include <ydb/core/tablet_flat/flat_page_index.h>
#include <ydb/core/tablet_flat/flat_part_charge_create.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include "ydb/core/tablet_flat/flat_part_btree_index_iter.h"
#include "ydb/core/tablet_flat/test/libs/table/wrap_iter.h"
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_steps.h>

namespace NKikimr {
namespace NTable {

class TPartBtreeIndexIt2 {
    using TCells = NPage::TCells;
    using TBtreeIndexNode = NPage::TBtreeIndexNode;
    using TGroupId = NPage::TGroupId;
    using TRecIdx = NPage::TRecIdx;
    using TChild = TBtreeIndexNode::TChild;
    using TBtreeIndexMeta = NPage::TBtreeIndexMeta;

public:
    TPartBtreeIndexIt2(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , GroupInfo(part->Scheme->GetLayout(groupId))
        , Meta(groupId.IsHistoric() ? part->IndexPages.BTreeHistoric[groupId.Index] : part->IndexPages.BTreeGroups[groupId.Index])
    {
    }
    
    EReady Seek(TRowId rowId) {
        TPageId pageId = Meta.PageId;

        for (ui32 level = 0; level < Meta.LevelCount; level++) {
            auto page = Env->TryGetPage(Part, pageId);
            if (!page) {
                return EReady::Page;
            }

            TBtreeIndexNode node(*page);
            auto pos = node.Seek(rowId);
            pageId = node.GetShortChild(pos).PageId;
        }

        return EReady::Data;
    }

private:
    const TPart* const Part;
    IPages* const Env;
    const TGroupId GroupId;
    const TPartScheme::TGroupInfo& GroupInfo;
    const TBtreeIndexMeta Meta;
};

namespace {
    using namespace NTest;

    using TCheckIt = TChecker<TWrapIter, TSubset>;
    using TCheckReverseIt = TChecker<TWrapReverseIter, TSubset>;

    NPage::TConf PageConf(size_t groups, bool writeBTreeIndex) noexcept
    {
        NPage::TConf conf{ true, 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).PageSize = 1024;
            conf.Group(group).BTreeIndexNodeTargetSize = 1024;
            // conf.Group(group).BTreeIndexNodeKeysMin = Max<ui32>();
        }

        conf.WriteBTreeIndex = writeBTreeIndex;

        conf.SliceSize = conf.Group(0).PageSize * 4;

        return conf;
    }

    struct TPartIndexSeekFixture : public benchmark::Fixture {
        using TGroupId = NPage::TGroupId;

        void SetUp(const ::benchmark::State& state) 
        {
            const bool groups = state.range(1);

            TLayoutCook lay;

            lay
                .Col(0, 0,  NScheme::NTypeIds::Uint32)
                .Col(0, 1,  NScheme::NTypeIds::Uint32)
                .Col(0, 2,  NScheme::NTypeIds::Uint32)
                .Col(0, 3,  NScheme::NTypeIds::Uint32)
                .Col(groups ? 1 : 0, 4,  NScheme::NTypeIds::Uint32)
                .Key({0, 1, 2});

            TPartCook cook(lay, PageConf(groups ? 2 : 1, true));
            
            for (ui32 i = 0; (groups ? cook.GetDataBytes(0) + cook.GetDataBytes(1) : cook.GetDataBytes(0)) < 100ull*1024*1024; i++) {
                cook.Add(*TSchemedCookRow(*lay).Col(i / 10000, i / 100 % 100, i % 100, i, i));
            }

            Eggs = cook.Finish();

            const auto part = Eggs.Lone();

            Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
            Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.Groups[groups ? 1 : 0], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeGroups[groups ? 1 : 0].IndexSize << Endl;
            Cerr << "Levels = " << part->IndexPages.BTreeGroups[groups ? 1 : 0].LevelCount << Endl;

            // 100 MB
            UNIT_ASSERT_GE(part->Stat.Bytes, 100ull*1024*1024);
            UNIT_ASSERT_LE(part->Stat.Bytes, 100ull*1024*1024 + 10ull*1024*1024);

            GroupId = TGroupId(groups ? 1 : 0);
        }

        TPartEggs Eggs;
        TTestEnv Env;
        TGroupId GroupId;
    };

    struct TPartIndexIteratorFixture : public benchmark::Fixture {
        using TGroupId = NPage::TGroupId;

        void SetUp(const ::benchmark::State& state) 
        {
            const bool useBTree = state.range(0);
            const bool groups = state.range(1);
            const bool history = state.range(2);

            Mass = new NTest::TMass(new NTest::TModelStd(groups), history ? 1000000 : 300000);
            Subset = TMake(*Mass, PageConf(Mass->Model->Scheme->Families.size(), useBTree)).Mixed(0, 1, TMixerOne{ }, history ? 0.7 : 0);
            
            if (history) {
                Checker = new TCheckIt(*Subset, {new TTestEnv()}, TRowVersion(0, 8));
                CheckerReverse = new TCheckReverseIt(*Subset, {new TTestEnv()}, TRowVersion(0, 8));
            } else {
                Checker = new TCheckIt(*Subset, {new TTestEnv()});
                CheckerReverse = new TCheckReverseIt(*Subset, {new TTestEnv()});
            }
        }

        TMersenne<ui64> Rnd;
        TAutoPtr<NTest::TMass> Mass;
        TAutoPtr<TSubset> Subset;
        TAutoPtr<TCheckIt> Checker;
        TAutoPtr<TCheckReverseIt> CheckerReverse;
    };
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, GetIndexPage)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    if (useBTree) {
        auto pageId = Eggs.Lone()->IndexPages.BTreeGroups[GroupId.Index].PageId;
        auto page = Env.TryGetPage(Eggs.Lone().Get(), pageId, {});
        {
            // skip root
            NPage::TBtreeIndexNode node(*page);
            pageId = node.GetShortChild(0).PageId;
        }
        for (auto _ : state) {
            Env.TryGetPage(Eggs.Lone().Get(), pageId, {});
        }
    } else {
        auto pageId = Eggs.Lone()->IndexPages.Groups[GroupId.Index];
        for (auto _ : state) {
            Env.TryGetPage(Eggs.Lone().Get(), pageId, {});
        }
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, ParseIndexPage)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    if (useBTree) {
        auto pageId = Eggs.Lone()->IndexPages.BTreeGroups[GroupId.Index].PageId;
        auto page = Env.TryGetPage(Eggs.Lone().Get(), pageId, {});
        {
            // skip root
            NPage::TBtreeIndexNode node(*page);
            page = Env.TryGetPage(Eggs.Lone().Get(), node.GetShortChild(0).PageId, {});
        }
        
        for (auto _ : state) {
            NPage::TBtreeIndexNode node(*page);
            node.GetKeysCount();
        }
    } else {
        auto pageId = Eggs.Lone()->IndexPages.Groups[GroupId.Index];
        auto page = Env.TryGetPage(Eggs.Lone().Get(), pageId, {});
        for (auto _ : state) {
            NPage::TIndex index(*page);
            index.GetEndRowId();
        }
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, NodeSeek)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    if (useBTree) {
        auto pageId = Eggs.Lone()->IndexPages.BTreeGroups[GroupId.Index].PageId;
        auto page = Env.TryGetPage(Eggs.Lone().Get(), pageId, {});
        NPage::TBtreeIndexNode node(*page);
        // skip root
        page = Env.TryGetPage(Eggs.Lone().Get(), node.GetShortChild(0).PageId, {});
        auto end = node.GetShortChild(0).RowCount;
        node = NPage::TBtreeIndexNode(*page);
        Cerr << "node " << node.GetKeysCount() << Endl;
        for (auto _ : state) {
            node.Seek(RandomNumber<ui32>(end));
        }
    } else {
        auto pageId = Eggs.Lone()->IndexPages.Groups[GroupId.Index];
        auto page = Env.TryGetPage(Eggs.Lone().Get(), pageId, {});
        NPage::TIndex index(*page);
        Cerr << "index " << index->Count << Endl;
        for (auto _ : state) {
            index.LookupRow(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));
        }
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, SeekRowId)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    if (useBTree) {
        for (auto _ : state) {
            TPartBtreeIndexIt2 iter(Eggs.Lone().Get(), &Env, GroupId);
            iter.Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));
        }
    } else {
        for (auto _ : state) {
            TPartIndexIt iter(Eggs.Lone().Get(), &Env, GroupId);
            iter.Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));
        }
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, Next)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    THolder<IIndexIter> iter;

    if (useBTree) {
        iter = MakeHolder<TPartBtreeIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    } else {
        iter = MakeHolder<TPartIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    }

    iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));

    for (auto _ : state) {
        if (!iter->IsValid()) {
            iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));
        }
        iter->Next();
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, Prev)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    THolder<IIndexIter> iter;

    if (useBTree) {
        iter = MakeHolder<TPartBtreeIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    } else {
        iter = MakeHolder<TPartIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    }

    iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));

    for (auto _ : state) {
        if (!iter->IsValid()) {
            iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));
        }
        iter->Prev();
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, SeekKey)(benchmark::State& state) {
    const bool useBTree = state.range(0);
    const ESeek seek = ESeek(state.range(2));

    for (auto _ : state) {
        THolder<IIndexIter> iter;

        if (useBTree) {
            iter = MakeHolder<TPartBtreeIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
        } else {
            iter = MakeHolder<TPartIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
        }

        ui32 rowId = RandomNumber<ui32>(Eggs.Lone()->Stat.Rows);
        TVector<TCell> key{TCell::Make(rowId / 10000), TCell::Make(rowId / 100 % 100), TCell::Make(rowId % 100)};
        iter->Seek(seek, key, Eggs.Scheme->Keys.Get());
    }
}

BENCHMARK_DEFINE_F(TPartIndexIteratorFixture, DoReads)(benchmark::State& state) {
    const bool reverse = state.range(3);
    const ESeek seek = static_cast<ESeek>(state.range(4));
    const ui32 items = state.range(5);

    for (auto _ : state) {
        auto it = Mass->Saved.Any(Rnd);

        if (reverse) {
            CheckerReverse->Seek(*it, seek);
            for (ui32 i = 1; CheckerReverse->GetReady() == EReady::Data && i < items; i++) {
                CheckerReverse->Next();
            }
        } else {
            Checker->Seek(*it, seek);
            for (ui32 i = 1; Checker->GetReady() == EReady::Data && i < items; i++) {
                Checker->Next();
            }
        }
    }
}

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, GetIndexPage)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, ParseIndexPage)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, NodeSeek)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, SeekRowId)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, Next)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, Prev)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, SeekKey)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1},
        /* ESeek: */ {0, 1, 2}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexIteratorFixture, DoReads)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {1},
        /* history: */ {1},
        /* reverse: */ {0},
        /* ESeek: */ {1},
        /* items */ {1, 10, 100}})
    ->Unit(benchmark::kMicrosecond);

}
}
