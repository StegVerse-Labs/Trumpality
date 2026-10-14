#!/usr/bin/env python3
"""Trajectory-scoped ERL discovery adapter for Trumpality.

Writes append-only lead-only candidate packets. Native Trumpality records and evaluation
labels are never mutated. Candidate packets conform to ERL transport v1; applicable
credential authority remains TV/TVC and GitHub token authority is NONE.

Every cycle is an attempted state transition: it appends one disposition record (ALLOW,
DENY or FAIL_CLOSED) to research/transition_dispositions.jsonl.
"""
from __future__ import annotations
import argparse,csv,hashlib,json,pathlib,re,time,urllib.error,urllib.request
from datetime import datetime,timezone
from html.parser import HTMLParser
from urllib.parse import urljoin,urlsplit
REPOSITORY='StegVerse-Labs/Trumpality';HOST_SPACING=3;RETRY_DELAY=10;UA='StegVerse-ERL-Trumpality/1.1'
OWNING_GOAL='StegVerse-Labs/Trumpality#8';RETRY_ENTRYPOINT='.github/workflows/erl-active-research.yml -> python scripts/erl_research_agent.py'
DISPOSITION_SCHEMA='stegverse.trumpality.erl_acquisition_transition_disposition.v1'
def now():return datetime.now(timezone.utc).isoformat()
def sid(*p):return hashlib.sha256('|'.join(map(str,p)).encode()).hexdigest()[:24]
def file_sha(path):return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
def append(path,obj,dry):
    if dry:return
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a',encoding='utf-8') as f:f.write(json.dumps(obj,sort_keys=True)+'\n')
def jsonl(path):return [json.loads(x) for x in path.read_text(encoding='utf-8').splitlines() if x.strip()] if path.exists() else []
def load(path):return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
def sources(path):
    if not path.exists():return []
    with path.open(newline='',encoding='utf-8') as f:return list(csv.DictReader(f))
def dispose(base,dry,cycle_id,predecessor,disposition,**fields):
    record={'schema':DISPOSITION_SCHEMA,'transition_id':'TRN-'+sid(cycle_id,disposition),'cycle_id':cycle_id,'repository':REPOSITORY,'owning_existing_goal':OWNING_GOAL,'requested_action':'ACQUIRE_TRAJECTORY_SCOPED_SOURCE_CANDIDATES','predecessor':predecessor,'proposed_successor':'this cycle appended to research/research_receipts.jsonl and research/source_candidates.jsonl','disposition':disposition,'consequence_committed':disposition=='ALLOW','observed_at':now(),'evaluation_authority_effect':'NONE'}
    record.update(fields);append(base/'research/transition_dispositions.jsonl',record,dry);return record
def fail_closed(base,dry,cycle_id,predecessor,code,predicate,repair,message):
    dispose(base,dry,cycle_id,predecessor,'FAIL_CLOSED',failure_code=code,failed_predicate=predicate,required_evidence_or_repair=repair,retry_entrypoint=RETRY_ENTRYPOINT,next_attempt='rerun retry_entrypoint after the repair')
    raise RuntimeError('FAIL_CLOSED: '+message)
class Links(HTMLParser):
    def __init__(self):super().__init__();self.links=[];self.href=None;self.txt=[]
    def handle_starttag(self,tag,attrs):
        if tag=='a':self.href=dict(attrs).get('href');self.txt=[]
    def handle_data(self,d):
        if self.href is not None:self.txt.append(d)
    def handle_endtag(self,tag):
        if tag=='a' and self.href is not None:self.links.append((' '.join(self.txt).strip(),self.href));self.href=None;self.txt=[]
def feed_links(data):
    """Return (title, link) pairs from RSS/Atom items; empty for non-feed bodies."""
    import xml.etree.ElementTree as ET
    try:root=ET.fromstring(data)
    except ET.ParseError:return []
    out=[]
    for el in root.iter():
        if el.tag.rsplit('}',1)[-1] not in {'item','entry'}:continue
        title=link=summary=''
        for ch in el:
            name=ch.tag.rsplit('}',1)[-1]
            if name=='title':title=(ch.text or '').strip()
            elif name=='link':link=(ch.text or '').strip() or ch.get('href','')
            elif name in {'description','summary'}:summary=re.sub(r'<[^>]+>',' ',ch.text or '')
        if link:out.append((title,link,summary))
    return out
GENERIC={'and','the','for','with','records','record','events','event','evidence','data','series','official','public','map','controls','control','observations','timestamps','timestamp','windows','window','measures','support','matched','periods','snapshots','white','house','treasury','sec'}
def query_terms(q):
    return [t for t in dict.fromkeys(x.lower() for x in re.findall(r'[A-Za-z0-9][A-Za-z0-9._-]{2,}',q)) if t not in GENERIC][:12]
def matches(terms,hay,subject=None):
    if subject:
        # A lead must name the trajectory's subject and touch the evidence sought.
        evidence=[t for t in terms if t not in subject]
        # Evidence terms match at a word start ("wallet" matches "wallets", "graph" does not match "photographer").
        return any(re.search(r'(?<![a-z0-9])'+re.escape(s)+r'(?![a-z0-9])',hay) for s in subject) and (not evidence or any(re.search(r'(?<![a-z0-9])'+re.escape(t),hay) for t in evidence))
    # Require two distinct terms when the query offers two, so one generic word cannot create a lead.
    return bool(terms) and sum(t in hay for t in terms)>=min(2,len(terms))
def active(base):
    out=jsonl(base/'research/acquisition_requests.jsonl');f=load(base/'research/frontier.json')
    for t in f.get('trajectories',[]):
        if t.get('state') not in {'OPEN','ACTIVE'}:
            continue
        trajectory_id=t.get('trajectory_id') or t.get('id')
        if not trajectory_id:
            raise ValueError('Active frontier trajectory missing identifier')
        queries=t.get('acquisition_queries')
        if queries is None:
            queries=t.get('required_evidence',[])
        if not queries:
            raise ValueError('Active frontier trajectory has no acquisition queries or evidence targets: '+trajectory_id)
        for q in queries:
            if not isinstance(q,str) or not q.strip():
                raise ValueError('Invalid acquisition query for '+trajectory_id)
            req={'request_id':'frontier-'+sid(trajectory_id,q),'trajectory_ids':[trajectory_id],'query':q,'state':'ACTIVE'}
            if t.get('subject_terms'):req['subject_terms']=[x.lower() for x in t['subject_terms']]
            out.append(req)
    unique={}
    for r in out:
        if r.get('state','ACTIVE') in {'OPEN','ACTIVE','RETRY'}:
            unique.setdefault(r.get('request_id'),r)
    return list(unique.values())
def packet(req,src,title,link,listing_sha256=None,receipt_id=None):return {'schema':'stegverse.erl.research_source_candidate.v1','candidate_id':'SRC-'+sid(req.get('request_id'),link),'repository':REPOSITORY,'trajectory_ids':req.get('trajectory_ids',[]),'acquisition_request_id':req.get('request_id'),'query':req.get('query',''),'source_url':link,'source_title':title,'retrieved_at':now(),'source_class':src.get('authority_class') or src.get('type') or 'unknown','authority_proximity':'unknown','content_sha256':None,'listing_response_sha256':listing_sha256,'custody_pointer':receipt_id,'verification_state':'UNVERIFIED','evidence_role':'lead-only','native_records_mutated':False,'evaluation_changed':False,'discovered_by':'scripts/erl_research_agent.py','transport':{'source_repository':REPOSITORY,'destination_repository':'StegVerse-Labs/Executive_Rhetoric_Ledger','authority_effect':'NONE','credential_authority':'TV/TVC','github_token_authority':'NONE'}}
def main():
    p=argparse.ArgumentParser();p.add_argument('--base',default='.');p.add_argument('--dry-run',action='store_true');a=p.parse_args();b=pathlib.Path(a.base).resolve();R=active(b);S=sources(b/'data/sources/sources_whitelist.csv');count=0;seen=set()
    receipts_path=b/'research/research_receipts.jsonl';candidates_path=b/'research/source_candidates.jsonl'
    prior_receipts=jsonl(receipts_path)
    # Keep scan evidence per cycle even when the same source body is retrieved again.
    cycle_id=sid(now(),len(prior_receipts))
    predecessor={'research_receipts_sha256':file_sha(receipts_path),'source_candidates_sha256':file_sha(candidates_path),'research_receipt_count':len(prior_receipts)}
    if not R:
        dispose(b,a.dry_run,cycle_id,predecessor,'DENY',failure_code='NO_ACTIVE_ACQUISITION_REQUESTS',failed_predicate='research/frontier.json or research/acquisition_requests.jsonl yields at least one OPEN/ACTIVE/RETRY request',required_evidence_or_repair='activate a frontier trajectory or append an ACTIVE acquisition request',retry_entrypoint=RETRY_ENTRYPOINT,next_attempt='rerun retry_entrypoint after a request is active')
    if R and not S:
        fail_closed(b,a.dry_run,cycle_id,predecessor,'SOURCE_INVENTORY_MISSING','data/sources/sources_whitelist.csv exists and lists at least one source','add governed public sources to data/sources/sources_whitelist.csv','source inventory missing or empty; acquisition not executed')
    if R and not any(urlsplit((src.get('url') or '').strip()).scheme in {'http','https'} and urlsplit((src.get('url') or '').strip()).hostname for src in S):
        fail_closed(b,a.dry_run,cycle_id,predecessor,'SOURCE_INVENTORY_UNUSABLE','at least one source in data/sources/sources_whitelist.csv has an HTTP(S) URL with a host','correct or replace the source URLs in data/sources/sources_whitelist.csv','source inventory has no usable HTTP(S) endpoint; acquisition not executed')
    existing_candidates={r.get('candidate_id') for r in jsonl(candidates_path)}
    successful_scans=0;fetched={};last_hit={}
    for src in S:
        u=(src.get('url') or '').strip()
        if not u or urlsplit(u).scheme not in {'http','https'} or not urlsplit(u).hostname:continue
        # Space requests to one host; back-to-back Federal Register queries are throttled.
        host=urlsplit(u).hostname;wait=last_hit.get(host,0)+HOST_SPACING-time.monotonic()
        if wait>0:time.sleep(wait)
        last_hit[host]=time.monotonic()
        # One retrieval per source per cycle; every request is matched against the same bytes.
        try:
            try:data=urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':UA}),timeout=15).read(2_000_000)
            except urllib.error.HTTPError as e:
                if e.code not in {403,429,500,502,503,504}:raise
                # One spaced retry for throttling; a second failure is recorded as FAILED.
                time.sleep(RETRY_DELAY);data=urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':UA}),timeout=15).read(2_000_000)
            links=feed_links(data)
            if not links:parser=Links();parser.feed(data.decode('utf-8',errors='ignore'));links=parser.links
            fetched[u]=(hashlib.sha256(data).hexdigest(),links,None)
        except Exception as e:fetched[u]=(None,[],type(e).__name__)
    for req in R:
        terms=query_terms(req.get('query',''))
        for src in S:
            u=(src.get('url') or '').strip()
            if u not in fetched:continue
            digest,links,error=fetched[u]
            if error:
                append(receipts_path,{'receipt_id':'RSRCH-'+sid(cycle_id,req.get('request_id'),u,'FAILED'),'request_id':req.get('request_id'),'trajectory_ids':req.get('trajectory_ids',[]),'source_scanned':u,'retrieved_at':now(),'result':'FAILED','error':error,'native_records_mutated':False,'evaluation_changed':False},a.dry_run)
                continue
            hits=[]
            for title,href,*summary in links:
                link=urljoin(u,href)
                # Match title, feed summary and URL path; the source's own hostname is not evidence of relevance.
                hay=' '.join([title,*summary,urlsplit(link).path.replace('-',' ')]).lower()
                if not title or not matches(terms,hay,req.get('subject_terms')):continue
                k=sid(link)
                if (req.get('request_id'),k) in seen:continue
                seen.add((req.get('request_id'),k));hits.append((title,link))
            receipt_id='RSRCH-'+sid(cycle_id,req.get('request_id'),u,digest)
            for title,link in hits[:10]:
                candidate=packet(req,src,title,link,digest,receipt_id)
                if candidate['candidate_id'] in existing_candidates:continue
                append(candidates_path,candidate,a.dry_run)
                existing_candidates.add(candidate['candidate_id'])
                count+=1
            successful_scans+=1
            append(receipts_path,{'receipt_id':receipt_id,'cycle_id':cycle_id,'request_id':req.get('request_id'),'trajectory_ids':req.get('trajectory_ids',[]),'query_terms':terms,'source_scanned':u,'retrieved_at':now(),'response_hash':digest,'hits':len(hits),'result':'NO_UPDATE' if not hits else 'CANDIDATES_IDENTIFIED','native_records_mutated':False,'evaluation_changed':False},a.dry_run)
    failed_sources=sorted(u for u,v in fetched.items() if v[2])
    if R and successful_scans==0:
        fail_closed(b,a.dry_run,cycle_id,predecessor,'ALL_SOURCE_SCANS_FAILED','at least one configured source returns a readable response','inspect the FAILED receipts appended this cycle (error per source) and repair or replace those sources: '+', '.join(failed_sources),'all configured source scans failed; inspect appended FAILED receipts')
    if R:
        dispose(b,a.dry_run,cycle_id,predecessor,'ALLOW',evidence={'requests':len(R),'sources':len(S),'sources_fetched':len(fetched)-len(failed_sources),'failed_sources':failed_sources,'scans':successful_scans,'candidates_appended':count,'research_receipts_sha256_after':file_sha(receipts_path),'source_candidates_sha256_after':file_sha(candidates_path)})
    print(json.dumps({'repository':REPOSITORY,'requests':len(R),'sources':len(S),'sources_fetched':sum(1 for v in fetched.values() if not v[2]),'scans':successful_scans,'candidates':count,'dry_run':a.dry_run,'native_records_mutated':False,'evaluation_changed':False,'candidate_schema':'stegverse.erl.research_source_candidate.v1','credential_authority':'TV/TVC','github_token_authority':'NONE'},sort_keys=True))
if __name__=='__main__':main()
