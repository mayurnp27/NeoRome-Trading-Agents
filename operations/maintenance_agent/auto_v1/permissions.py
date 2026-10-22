"""Codex named permissions: model tools see only their workspace and OS tools."""
def flags(*,write):
    values={
        'default_permissions':'"maintenance"',
        'permissions.maintenance.extends':'":workspace"' if write else '":read-only"',
        'permissions.maintenance.filesystem':('{":root"="deny",":minimal"="read",":workspace_roots"="'+('write' if write else 'read')+'","/opt/neorome-codex"="read","/var/lib/neorome-maintenance-ai/home"="deny"}'),
        'permissions.maintenance.network.enabled':'false',
        'web_search':'"disabled"',
    }
    return [part for key,value in values.items() for part in ('-c',key+'='+value)]
