"""手动更新米筐市场底座：只在显式--execute时取数并入库。"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from riskaudit.market_foundation.manual_increment import main

if __name__ == "__main__":
    main()
