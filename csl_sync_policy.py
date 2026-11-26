"""Local upload choices. Changing a choice never deletes remote sessions."""
from datetime import datetime
from pathlib import Path

from csl_sync_state import SyncError, now


def configure(state, selection, *, limit=None, folders=(), rows=()):
    if selection not in ('new', 'last', 'all', 'folders'):
        raise SyncError('invalid_selection_policy')
    if (selection == 'last' and (type(limit) is not int or limit <= 0)) or (selection != 'last' and limit is not None):
        raise SyncError('invalid_selection_limit')
    if bool(folders) != (selection == 'folders'):
        raise SyncError('invalid_selection_folders')
    policy = dict(selection=selection, configured_at=now(), limit=limit,
                  folders=[str(Path(folder).expanduser().resolve()) for folder in folders])
    # Snapshot last N once, across providers; later sessions follow the new cutoff.
    if selection == 'last':
        eligible = [row for row in rows if not row.get('error') and row.get('updated_at')]
        def activity(row):
            try: return datetime.fromisoformat(row['updated_at'].replace('Z', '+00:00')).timestamp()
            except (ValueError, TypeError, AttributeError): return float('-inf')
        eligible.sort(key=lambda row: (activity(row), row['provider'], row['native_id']), reverse=True)
        policy['initial_sessions'] = [[row['provider'], row['native_id']] for row in eligible[:limit]]
    state.set('selection_policy', policy)
    state.set('selected_sessions', [])
    return policy


def selected(policy, provider, row):
    kind = policy['selection']
    if kind == 'all': return True
    if kind == 'folders':
        cwd = row.get('cwd')
        if not isinstance(cwd, str) or not cwd: return False
        path = Path(cwd).expanduser().resolve()
        return any(path.is_relative_to(Path(folder)) for folder in policy['folders'])
    if kind == 'last' and [provider, row['native_id']] in policy.get('initial_sessions', []):
        return True
    try:
        created = datetime.fromisoformat(row['created_at'].replace('Z', '+00:00'))
        cutoff = datetime.fromisoformat(policy['configured_at'].replace('Z', '+00:00'))
        return created >= cutoff
    except (ValueError, TypeError, KeyError, AttributeError):
        return False
