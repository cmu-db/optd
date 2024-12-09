import os
from pathlib import Path

def tbl_to_csv(file):
    lines = []
    for line in Path(file).read_text().splitlines():
        # Replace the delimiter `|` with `,`
        line = line.strip('|')
        lines.append(line)
    # Write the converted content to a new `.csv` file
    Path(file.replace('.tbl', '.csv')).write_text('\n'.join(lines))

def main():
    # Find all files end with `.tbl` in the current directory
    # and convert them to `.csv` files.
    for file in os.listdir('.'):
        if file.endswith('.tbl'):
            tbl_to_csv(file)


if __name__ == '__main__':
    main()
