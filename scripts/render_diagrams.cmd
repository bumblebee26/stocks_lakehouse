@echo off
rem Re-render the architecture diagrams (docs\diagrams\*.mmd) to SVG and PNG.
rem Needs the Mermaid CLI: npm install -g @mermaid-js/mermaid-cli
cd /d "%~dp0..\docs\diagrams"
for %%f in (*.mmd) do (
  call mmdc -i "%%f" -o "%%~nf.svg" -c theme.json -b white -q
  call mmdc -i "%%f" -o "%%~nf.png" -c theme.json -b white -s 2 -q
  echo rendered %%~nf
)
