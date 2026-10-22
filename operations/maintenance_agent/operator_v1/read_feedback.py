#!/usr/bin/env python3
import json
from pathlib import Path
root=Path('/var/lib/neorome-maintenance-ai')
result={}
for key,path in [('desks',root/'inbox/desk-readiness.json'),('maintenance',root/'inbox/status.json'),('paper',root/'inbox/paper-feedback.json'),('remote',root/'state/remote-control.json'),('worker',root/'state/status.json')]:
 try:
  if path.is_symlink() or path.stat().st_size>1048576:raise ValueError('unsafe or oversized source')
  result[key]=json.loads(path.read_text())
 except (OSError,ValueError) as exc:result[key]={'status':'unavailable','error_class':type(exc).__name__}
print(json.dumps(result,indent=2))
