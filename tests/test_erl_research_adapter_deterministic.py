#!/usr/bin/env python3
from __future__ import annotations
import importlib.util,hashlib,json,pathlib,sys,tempfile
from unittest import mock
ROOT=pathlib.Path(__file__).resolve().parents[1]; ADAPTER=ROOT/'scripts'/'erl_research_agent.py'; REPOSITORY='StegVerse-Labs/Trumpality'
class FakeResponse:
    def __init__(self,body:bytes): self._body=body; self.headers={'Content-Type':'text/html'}
    def read(self,_limit=-1): return self._body
def load_adapter():
    spec=importlib.util.spec_from_file_location('erl_research_agent',ADAPTER); module=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(module); return module
def read_jsonl(path): return [json.loads(x) for x in path.read_text(encoding='utf-8').splitlines() if x.strip()]
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
    adapter=load_adapter()
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
    with tempfile.TemporaryDirectory() as invalid_dir:
        ib=pathlib.Path(invalid_dir); (ib/'research').mkdir(); (ib/'data/sources').mkdir(parents=True)
        (ib/'research/frontier.json').write_text(json.dumps({'trajectories':[{'id':'T1','state':'ACTIVE','required_evidence':['public record']}]}))
        (ib/'data/sources/sources_whitelist.csv').write_text('name,url,authority_class\ninvalid,not-a-web-address,official\n')
        with mock.patch.object(sys,'argv',[str(ADAPTER),'--base',str(ib)]):
            try: adapter.main()
            except RuntimeError as exc: assert 'FAIL_CLOSED' in str(exc) and 'usable HTTP(S)' in str(exc)
            else: raise AssertionError('Unusable source inventory must fail closed')
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
        print(json.dumps({'status':'PASS','repository':REPOSITORY,'active_trajectory_requests':1,'saturated_trajectory_requests':0,'candidates':2,'receipts':2,'supporting_and_contrary_leads_preserved':True,'null_result_preserved':True,'duplicate_links_collapsed':True,'native_state_unchanged':True,'github_token_authority':'NONE','credential_authority':'TV/TVC','authority_effect':'NONE'},sort_keys=True))
    return 0
if __name__=='__main__': raise SystemExit(main())
