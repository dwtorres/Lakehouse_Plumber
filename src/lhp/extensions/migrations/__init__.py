"""One-shot migration scripts for the watermark_manager package.

Each script is standalone and invokable via ``python -m``. They are not
auto-run at ``WatermarkManager`` startup — operators schedule them once
per environment during a Slice rollout.
"""
