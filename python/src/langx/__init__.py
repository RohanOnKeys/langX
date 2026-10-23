"""langx: LangGraph agent steps executed on HPX localities.

Research prototype. Worker failures are handled by HPX supervision fencing
plus a coordinator-side commit fence, so a step is neither lost nor committed
twice. See docs/plan.md for scope and claims.

This module never imports the native extension, so it stays importable
without HPX. The extension is loaded by ``langx.runtime`` only.
"""

__version__ = "0.0.0"
