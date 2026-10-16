from pathlib import Path
import os, shutil, subprocess, collections
root=Path(__file__).parent.resolve()
pg=root/'pg'
bundle=pg/'lib'/'bundle'
bundle.mkdir(exist_ok=True)
queue=collections.deque()
seen=set()
for part in ('bin','lib'):
    for p in (pg/part).rglob('*'):
        if p.is_file() and not p.is_symlink(): queue.append(p)
external={}
patched=0
while queue:
    path=queue.popleft()
    if path in seen: continue
    seen.add(path)
    result=subprocess.run(['otool','-L',str(path)],capture_output=True,text=True)
    if result.returncode: continue
    deps=[line.strip().split(' (')[0] for line in result.stdout.splitlines()[1:]]
    ident_lines=subprocess.run(['otool','-D',str(path)],capture_output=True,text=True).stdout.splitlines()[1:]
    ident=ident_lines[0] if ident_lines else None
    changes=[]
    for dep in deps:
        if not dep.startswith('/opt/homebrew/') or dep == ident:
            continue
        source=Path(dep)
        if not source.exists(): raise RuntimeError(f'missing {dep} required by {path}')
        dest=bundle/source.name
        if not dest.exists():
            shutil.copy2(source,dest)
            dest.chmod(dest.stat().st_mode | 0o200)
            queue.append(dest)
        external[dep]=dest
        relative=os.path.relpath(dest,path.parent)
        changes += ['-change',dep,'@loader_path/'+relative]
    if changes:
        path.chmod(path.stat().st_mode | 0o200)
        p=subprocess.run(['install_name_tool',*changes,str(path)],capture_output=True,text=True)
        if p.returncode: raise RuntimeError(f'{path}: {p.stderr}')
        patched+=1
# ICU lists libicudata with @loader_path already, so it is absent from the
# absolute-reference queue even though Homebrew installs it separately.
source=Path('/opt/homebrew/opt/icu4c@78/lib/libicudata.78.dylib')
dest=bundle/source.name
if not dest.exists():
    shutil.copy2(source,dest)
    dest.chmod(dest.stat().st_mode | 0o200)

signed=0
for part in ('bin','lib'):
    for path in (pg/part).rglob('*'):
        if not path.is_file() or path.is_symlink(): continue
        kind=subprocess.run(['file','-b',str(path)],capture_output=True,text=True).stdout
        if 'Mach-O' not in kind: continue
        p=subprocess.run(['codesign','--force','--sign','-',str(path)],capture_output=True,text=True)
        if p.returncode: raise RuntimeError(f'codesign {path}: {p.stderr}')
        signed+=1
print(f'processed={len(seen)} patched={patched} copied_dylibs={len(list(bundle.glob("*")))} signed={signed}')
