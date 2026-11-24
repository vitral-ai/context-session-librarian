"""Restricted GitHub workflow completion notices; no repository code is executed."""
import json
import os
import re
import ssl
import urllib.request
import uuid

from csl_bridge_store import BridgeError, name


def notice_request(policy, notice):
    """Build the only envelope a repository notice credential may enqueue."""
    fields = {'action', 'repository', 'number', 'branch', 'workflow', 'run_id', 'attempt', 'sha', 'results'}
    if set(notice) != fields or notice['action'] != 'notice':
        raise BridgeError('Notice credentials accept completion metadata only')
    repo = notice['repository']
    if repo != policy['repository'] or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise BridgeError('Notice repository does not match credential')
    for key in ('number', 'run_id', 'attempt'):
        if type(notice[key]) is not int or not 0 < notice[key] < 2**63:
            raise BridgeError('Invalid notice ' + key)
    branch, workflow, sha, results = (notice[k] for k in ('branch', 'workflow', 'sha', 'results'))
    if not isinstance(branch, str) or not 0 < len(branch) <= 255:
        raise BridgeError('Invalid notice branch')
    if not isinstance(workflow, str) or not re.fullmatch(r'[A-Za-z0-9 _.()/-]{1,120}', workflow):
        raise BridgeError('Invalid notice workflow')
    if not isinstance(sha, str) or not re.fullmatch(r'[0-9a-f]{40}', sha):
        raise BridgeError('Invalid notice head SHA')
    if not isinstance(results, dict) or not 0 < len(results) <= 100 or any(
            not isinstance(k, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', k) or
            v not in ('success', 'failure', 'cancelled', 'skipped') for k, v in results.items()):
        raise BridgeError('Invalid notice job results')
    target = policy.get('routes', {}).get(branch)
    if not target:
        match = re.fullmatch(r'codex/([a-zA-Z0-9][a-zA-Z0-9_.-]*)/.+', branch)
        target = match.group(1) if match else None
    if not target:
        raise BridgeError('No agent mapping for PR branch; use codex/<agent>/...')
    name(target)
    if 'failure' in results.values():
        outcome = 'failed'
    elif 'cancelled' in results.values():
        outcome = 'cancelled'
    elif 'success' in results.values():
        outcome = 'passed'
    else:
        outcome = 'skipped'
    detail = ', '.join(k + '=' + v for k, v in sorted(results.items()))
    url = 'https://github.com/' + repo + '/actions/runs/' + str(notice['run_id']) + '/attempts/' + str(notice['attempt'])
    text = ('#' + str(notice['number']) + ': ' + workflow + ' ' + outcome + '. ' + detail +
            '\nHead: ' + sha + '\n' + url)
    # Reposting a completed run is idempotent; a deliberate rerun is a new completion.
    mid = str(uuid.uuid5(uuid.NAMESPACE_URL, url))
    return {'action': 'send', 'source': policy['source'], 'target': target, 'id': mid, 'text': text}


def workflow_notice(env):
    """Use GitHub's completed-run metadata, never candidate code or artifacts."""
    event = json.loads(env['CSL_EVENT'])
    run = event['workflow_run']
    repo = env['CSL_REPOSITORY']
    if run['head_repository']['full_name'] != repo:
        raise BridgeError('Fork PR notices do not receive a credential')
    if run['event'] not in ('pull_request', 'pull_request_target') or run['status'] != 'completed':
        raise BridgeError('Notice requires a completed PR workflow')
    prs = run['pull_requests']
    if len(prs) != 1 or prs[0]['head']['repo']['id'] != int(env['CSL_REPOSITORY_ID']):
        raise BridgeError('Completion must identify exactly one same-repository PR')
    pr = prs[0]
    return {'action': 'notice', 'repository': repo, 'number': pr['number'],
            'branch': pr['head']['ref'], 'sha': pr['head']['sha'],
            'workflow': run['name'], 'run_id': run['id'], 'attempt': run['run_attempt'],
            'results': {'completion': {'timed_out': 'failure', 'action_required': 'failure',
                                      'startup_failure': 'failure', 'neutral': 'skipped',
                                      'stale': 'cancelled'}.get(run['conclusion'], run['conclusion'])}}


def main():
    env = os.environ
    context = ssl.create_default_context(cadata=env['CSL_HUB_CA_PEM'])
    url = env['CSL_HUB_URL']
    if not url.startswith('https://') or not url.endswith('/v1'):
        raise BridgeError('Hub requires a pinned HTTPS /v1 endpoint')
    request = urllib.request.Request(url, data=json.dumps(workflow_notice(env)).encode(),
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + env['CSL_HUB_NOTICE_TOKEN']})
    # One request, no polling/retry loop. The hub durably owns delivery after enqueue.
    with urllib.request.urlopen(request, context=context, timeout=20) as response:
        result = json.load(response)
    if not result.get('ok'):
        raise BridgeError('Hub refused completion notice')
    print('CSL notice ' + result['data']['id'] + ': ' + result['data']['state'])


if __name__ == '__main__':
    main()
