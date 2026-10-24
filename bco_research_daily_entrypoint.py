"""BCO production daily-accounting app plus zero-authority protection research."""
import bco_daily_summary_entrypoint as base
import bco_protection_research_v2 as research

app = base.app

print(
    "BCO_RESEARCH_DAILY_ENTRYPOINT installed=True "
    f"research_version={research.VERSION} execution_authority=False",
    flush=True,
)
