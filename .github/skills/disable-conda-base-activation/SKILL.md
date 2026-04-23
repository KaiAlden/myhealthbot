---
name: disable-conda-base-activation
description: Disable automatic activation of conda base environment in VS Code terminals. Use when: PowerShell terminals automatically activate base environment and you want to stop this behavior.
---

# Disable Conda Base Activation Skill

This skill automates the process of disabling automatic activation of the conda base environment in VS Code PowerShell terminals.

## Workflow Steps

1. **Check conda auto_activate_base setting**: Run `conda config --show auto_activate_base` to see current value.
2. **Disable auto activation**: If true, run `conda config --set auto_activate_base false`.
3. **Check PowerShell profile**: Run `Test-Path $PROFILE` to check if profile exists.
4. **Create profile if missing**: If not exists, run `New-Item -Path $PROFILE -ItemType File -Force`.
5. **Check VS Code settings**: Read `settings.json` at `$env:USERPROFILE\AppData\Roaming\Code\User\settings.json`.
6. **Update settings**: Add or ensure `"python.terminal.activateEnvironment": false` is set.

## Post-Execution

- Restart VS Code for changes to take effect.
- New terminals will not automatically activate base environment.
- Manually activate desired environments as needed.