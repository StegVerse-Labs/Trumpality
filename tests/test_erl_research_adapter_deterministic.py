#!/usr/bin/env python3
from __future__ import annotations
import importlib.util,hashlib,json,pathlib,sys,tempfile
from unittest import mock
ROOT=pathlib.Path(__file__).resolve().parents[1]; ADAPTER=ROOT/'scripts'/'erl_research_agent.py'; REPOSITORY='StegVerse-Labs/Trumpality'
ACTIONABLE=('failure_code','failed_predicate','required_evidence_or_repair','retry_entrypoint','owning_existing_goal','next_attempt')
class FakeResponse:
    def __init__(self,body:bytes): self._body=body; self.headers={'Content-Type':'text/html'}
    def read(self,_limit=-1): return self._body
def load_adapter():
    spec=importlib.util.spec_from_file_location('erl_research_agent',ADAPTER); module=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(module); return module
def read_jsonl(path): return [json.loads(x) for x in path.read_text(encoding='utf-8').splitlines() if x.strip()]
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
    adapter=load_adapter(); adapter.HOST_SPACING=0; adapter.RETRY_DELAY=0
    canonical=json.loads((ROOT/'research/frontier.json').read_text(encoding='utf-8'))
    assert len([t for t in canonical['trajectories'] if t['state']=='ACTIVE'])==12
    with tempfile.TemporaryDirectory() as canonical_dir:
        cb=pathlib.Path(canonical_dir); (cb/'research').mkdir()
        (cb/'research/frontier.json').write_text(json.dumps(canonical),encoding='utf-8')
        requests=adapter.active(cb)
        expected=sum(len(t['required_evidence']) for t in canonical['trajectories'] if t['state']=='ACTIVE')
        assert len(requests)==expected and expected>12, (len(requests),expected)
        assert {tid for r in requests for tid in r['trajectory_ids']}=={t['id'] for t in canonical['trajectories'] if t['state']=='ACTIVE'}
        assert len({r['request_id'] for r in requests})==len(requests)
        assert adapter.active(cb)==requests, 'frontier mapping must be deterministic'
        with (cb/'research/acquisition_requests.jsonl').open('w',encoding='utf-8') as out:
            out.write(json.dumps(requests[0])+'\n')
        assert len(adapter.active(cb))==len(requests), 'duplicate persisted request must collapse'
    with tempfile.TemporaryDirectory() as missing_dir:
        mb=pathlib.Path(missing_dir); (mb/'research').mkdir()
        (mb/'research/frontier.json').write_text(json.dumps({'trajectories':[{'id':'T1','state':'ACTIVE','required_evidence':['public record']}]}))
        with mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(mb)]):
            try:
                adapter.main()
            except RuntimeError as exc:
                assert 'FAIL_CLOSED' in str(exc) and 'source inventory' in str(exc)
            else:
                raise AssertionError('Missing source inventory must fail closed')
        md=read_jsonl(mb/'research/transition_dispositions.jsonl')
        assert len(md)==1 and md[0]['disposition']=='FAIL_CLOSED' and md[0]['failure_code']=='SOURCE_INVENTORY_MISSING' and md[0]['consequence_committed'] is False
        assert all(md[0].get(k) for k in ACTIONABLE), 'a non-ALLOW disposition must be actionable, never a bare blocker'
    with tempfile.TemporaryDirectory() as invalid_dir:
        ib=pathlib.Path(invalid_dir); (ib/'research').mkdir(); (ib/'data/sources').mkdir(parents=True)
        (ib/'research/frontier.json').write_text(json.dumps({'trajectories':[{'id':'T1','state':'ACTIVE','required_evidence':['public record']}]}))
        (ib/'data/sources/sources_whitelist.csv').write_text('name,url,authority_class\ninvalid,not-a-web-address,official\n')
        with mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(ib)]):
            try: adapter.main()
            except RuntimeError as exc: assert 'FAIL_CLOSED' in str(exc) and 'usable HTTP(S)' in str(exc)
            else: raise AssertionError('Unusable source inventory must fail closed')
        assert [x['failure_code'] for x in read_jsonl(ib/'research/transition_dispositions.jsonl')]==['SOURCE_INVENTORY_UNUSABLE']
    with tempfile.TemporaryDirectory() as failed_dir:
        fb=pathlib.Path(failed_dir); (fb/'research').mkdir(); (fb/'data/sources').mkdir(parents=True)
        (fb/'research/frontier.json').write_text(json.dumps({'trajectories':[{'id':'T1','state':'ACTIVE','required_evidence':['public record']}]}))
        (fb/'data/sources/sources_whitelist.csv').write_text('name,url,authority_class\nfailed,https://fixture.local/fails,official\n')
        with mock.patch.object(adapter.urllib.request,'urlopen',side_effect=OSError('offline')), mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(fb)]):
            try: adapter.main()
            except RuntimeError as exc: assert 'FAIL_CLOSED' in str(exc) and 'all configured source scans failed' in str(exc)
            else: raise AssertionError('All-failed acquisition cycle must fail closed')
        failed=read_jsonl(fb/'research/research_receipts.jsonl')
        assert len(failed)==1 and failed[0]['result']=='FAILED', 'failure receipt must persist'
        fd=read_jsonl(fb/'research/transition_dispositions.jsonl')
        assert [x['failure_code'] for x in fd]==['ALL_SOURCE_SCANS_FAILED'] and 'https://fixture.local/fails' in fd[0]['required_evidence_or_repair']
        assert all(fd[0].get(k) for k in ACTIONABLE)
    with tempfile.TemporaryDirectory() as td:
        b=pathlib.Path(td); (b/'research').mkdir(parents=True); (b/'data/sources').mkdir(parents=True); (b/'data/native').mkdir(parents=True)
        sentinel=b/'data/native/sentinel.json'; sentinel.write_text('{"native":"unchanged"}',encoding='utf-8'); before=sha(sentinel)
        (b/'research/frontier.json').write_text(json.dumps({'trajectories':[{'trajectory_id':'T1','state':'ACTIVE','acquisition_queries':['alpha beta']},{'trajectory_id':'T2','state':'SATURATED','acquisition_queries':['alpha beta']}]}))
        for p in ['acquisition_requests.jsonl','source_candidates.jsonl','research_receipts.jsonl']:(b/'research'/p).write_text('')
        (b/'data/sources/sources_whitelist.csv').write_text('name,url,authority_class\npositive,https://fixture.local/positive,official\nnull,https://fixture.local/null,official\n')
        positive=b'<a href="/alpha-beta-support">alpha beta supporting record</a><a href="/alpha-beta-support">alpha beta supporting record</a><a href="/alpha-beta-contrary">alpha beta contrary record</a>'
        null=b'<a href="/unrelated">unrelated record</a>'
        def fake(request,timeout=15): return FakeResponse(null if getattr(request,'full_url',str(request)).endswith('/null') else positive)
        with mock.patch.object(adapter.urllib.request,'urlopen',side_effect=fake), mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(b)]): adapter.main()
        c=read_jsonl(b/'research/source_candidates.jsonl'); r=read_jsonl(b/'research/research_receipts.jsonl')
        assert len(c)==2 and len(r)==2; assert {x['source_title'] for x in c}=={'alpha beta supporting record','alpha beta contrary record'}
        assert all(x['repository']==REPOSITORY and x['native_records_mutated'] is False and x['evaluation_changed'] is False for x in c)
        assert all(x['transport']['credential_authority']=='TV/TVC' and x['transport']['github_token_authority']=='NONE' and x['transport']['authority_effect']=='NONE' for x in c)
        assert any(x['result']=='NO_UPDATE' and x['hits']==0 for x in r); assert any(x['result']=='CANDIDATES_IDENTIFIED' and x['hits']==2 for x in r)
        # Repeated acquisition must preserve scan receipts but not duplicate candidate records.
        before_candidates=(b/'research/source_candidates.jsonl').read_bytes()
        before_receipts=(b/'research/research_receipts.jsonl').read_bytes()
        with mock.patch.object(adapter.urllib.request,'urlopen',side_effect=fake), mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(b)]): adapter.main()
        after_candidates=(b/'research/source_candidates.jsonl').read_bytes()
        after_receipts=(b/'research/research_receipts.jsonl').read_bytes()
        assert after_candidates==before_candidates, 'second cycle must not duplicate candidate records'
        assert after_receipts.startswith(before_receipts), 'second cycle must preserve prior receipt bytes'
        r2=read_jsonl(b/'research/research_receipts.jsonl')
        assert len(r2)==4 and len({x['receipt_id'] for x in r2})==4, 'each cycle requires unique scan receipts'
        assert sha(sentinel)==before, 'ERL sidecar must not mutate native Trumpality state'
        dd=read_jsonl(b/'research/transition_dispositions.jsonl')
        assert [x['disposition'] for x in dd]==['ALLOW','ALLOW'] and all(x['consequence_committed'] for x in dd)
        assert dd[-1]['evidence']['research_receipts_sha256_after']==sha(b/'research/research_receipts.jsonl')
        assert dd[1]['predecessor']['research_receipts_sha256']==dd[0]['evidence']['research_receipts_sha256_after'], 'each ALLOW must chain to the prior cycle state'
    with tempfile.TemporaryDirectory() as rd:
        rb=pathlib.Path(rd); (rb/'research').mkdir(); (rb/'data/sources').mkdir(parents=True)
        (rb/'research/frontier.json').write_text(json.dumps({'trajectories':[{'id':'T1','state':'ACTIVE','required_evidence':['stablecoin custody rules','announcement records']}]}))
        (rb/'data/sources/sources_whitelist.csv').write_text('name,type,url,authority_class\nfeed,rss,https://fixture.local/feed.xml,official-register\n')
        rss=b'<?xml version="1.0"?><rss><channel><item><title>Stablecoin custody rules adopted</title><link>https://fixture.local/doc/1</link></item><item><title>Unrelated notice</title><link>https://fixture.local/stablecoin-custody/2</link></item></channel></rss>'
        calls=[]
        def feed(request,timeout=15): calls.append(request.full_url); return FakeResponse(rss)
        with mock.patch.object(adapter.urllib.request,'urlopen',side_effect=feed), mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(rb)]): adapter.main()
        assert calls==['https://fixture.local/feed.xml'], 'each source must be fetched once per cycle regardless of request count'
        rc=read_jsonl(rb/'research/source_candidates.jsonl'); rr=read_jsonl(rb/'research/research_receipts.jsonl')
        assert [x['source_url'] for x in rc]==['https://fixture.local/doc/1','https://fixture.local/stablecoin-custody/2'], rc
        assert all(x['listing_response_sha256']==hashlib.sha256(rss).hexdigest() for x in rc)
        assert {x['custody_pointer'] for x in rc} <= {x['receipt_id'] for x in rr}, 'candidates must point at the scan receipt that produced them'
        assert len(rr)==2 and {x['result'] for x in rr}=={'CANDIDATES_IDENTIFIED','NO_UPDATE'}, rr
        assert adapter.query_terms('official SEC/Treasury/White House instruments')==['instruments'], 'source/institution names alone are not relevance evidence'
        assert not adapter.matches(['custody','rules'],'treasury custody notice'), 'one generic overlap must not create a lead'
    # Subject-gated trajectories: a lead must name the subject; subject words do not double as evidence.
    subject=['sanctions','stablecoin']
    assert adapter.matches(adapter.query_terms('sanctions/access constraints'),'treasury eases access constraints on sanctions program',subject)
    assert not adapter.matches(adapter.query_terms('sanctions/access constraints'),'sanctions programs and country information',subject), 'navigation naming only the subject is not a lead'
    assert not adapter.matches(adapter.query_terms('access constraints'),'bank access constraints rule',subject), 'evidence without subject is not a lead'
    assert not adapter.matches([],'unstablecoinish',['stablecoin']), 'subject terms match whole words only'
    with tempfile.TemporaryDirectory() as gd:
        gb=pathlib.Path(gd); (gb/'research').mkdir(); (gb/'data/sources').mkdir(parents=True)
        (gb/'research/frontier.json').write_text(json.dumps({'trajectories':[{'id':'T1','state':'ACTIVE','required_evidence':['custody rules'],'subject_terms':['Stablecoin']}]}))
        assert adapter.active(gb)[0]['subject_terms']==['stablecoin']
        (gb/'data/sources/sources_whitelist.csv').write_text('name,type,url,authority_class\nfeed,rss,https://fixture.local/throttled.xml,official-register\n')
        rss=b'<rss><channel><item><title>Stablecoin custody rules</title><link>https://fixture.local/a</link></item><item><title>Bank custody rules</title><link>https://fixture.local/b</link></item></channel></rss>'
        attempts=[]
        def throttled(request,timeout=15):
            attempts.append(1)
            if len(attempts)==1: raise adapter.urllib.error.HTTPError(request.full_url,429,'Too Many Requests',{},None)
            return FakeResponse(rss)
        with mock.patch.object(adapter.urllib.request,'urlopen',side_effect=throttled), mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(gb)]): adapter.main()
        assert len(attempts)==2, 'throttled source gets exactly one retry'
        assert [x['source_url'] for x in read_jsonl(gb/'research/source_candidates.jsonl')]==['https://fixture.local/a']
    # Feed summaries count toward matching: news headlines often omit the subject named in the summary.
    summary_feed=b'<rss><channel><item><title>Energy talks stall</title><link>https://fixture.local/n1</link><description>&lt;p&gt;Brent crude jumped after the remarks&lt;/p&gt;</description></item></channel></rss>'
    assert adapter.feed_links(summary_feed)==[('Energy talks stall','https://fixture.local/n1',' Brent crude jumped after the remarks ')]
    with tempfile.TemporaryDirectory() as sd:
        sb=pathlib.Path(sd); (sb/'research').mkdir(); (sb/'data/sources').mkdir(parents=True)
        (sb/'research/frontier.json').write_text(json.dumps({'trajectories':[{'id':'T1','state':'ACTIVE','required_evidence':['remarks timing'],'subject_terms':['crude']}]}))
        (sb/'data/sources/sources_whitelist.csv').write_text('name,type,url,authority_class\nnews,rss,https://fixture.local/news.xml,major-outlet\n')
        with mock.patch.object(adapter.urllib.request,'urlopen',return_value=FakeResponse(summary_feed)), mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(sb)]): adapter.main()
        assert [x['source_title'] for x in read_jsonl(sb/'research/source_candidates.jsonl')]==['Energy talks stall']
        print(json.dumps({'status':'PASS','repository':REPOSITORY,'active_trajectory_requests':1,'saturated_trajectory_requests':0,'candidates':2,'receipts':2,'supporting_and_contrary_leads_preserved':True,'null_result_preserved':True,'duplicate_links_collapsed':True,'native_state_unchanged':True,'actionable_non_allow_dispositions':True,'allow_transitions_chained':True,'github_token_authority':'NONE','credential_authority':'TV/TVC','authority_effect':'NONE'},sort_keys=True))
    return 0
if __name__=='__main__': raise SystemExit(main())
