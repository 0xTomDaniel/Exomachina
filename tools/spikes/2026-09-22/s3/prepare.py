"""Copy the retained native fixtures and regenerate their v2/latest variants."""
import argparse
import shutil
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output-dir', type=Path, required=True, help='External fixture output directory')
args = parser.parse_args()
source = Path(__file__).resolve().parent / 'fixtures'
repository = next((p for p in source.parents if (p / '.git').exists()), source.parent)
root = args.output_dir.expanduser().resolve()
if root.is_relative_to(repository):
    parser.error('Generated fixtures must be outside the repository')
root.mkdir(parents=True, exist_ok=True)
for fixture in source.glob('*.yaml'):
    shutil.copyfile(fixture, root / fixture.name)
pinned = (source / 'parent-pinned-v1.yaml').read_text()
(root / 'parent-pinned-v2.yaml').write_text(pinned.replace('revision: 1', 'revision: 2'))
(root / 'parent-latest.yaml').write_text(pinned.replace('s3_parent_pinned', 's3_parent_latest').replace('    revision: 1\n', ''))
reference = (source / 'reference-v1.yaml').read_text()
(root / 'reference-v2.yaml').write_text(reference.replace('revision: 1', 'revision: 2'))
print('Prepared native YAML v1/v2, pinned/latest child and bypass fixtures; no runtime access.')
