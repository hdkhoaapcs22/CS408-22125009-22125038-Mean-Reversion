# CS408-22125009-22125038-Mean-Reversion

Download two data folders(is and os) from the given link: https://drive.google.com/drive/folders/181d7JcfHilIvviLgEuaDt2VqwZLYnYUF

Create a **data** folder and paste the two downloaded folders into this folder.

## Run papertrade live
**Prerequisite**: Python virtual environment and dependencies are already in place and activated.

1. Download Python client package [here](https://papertrade.algotrade.vn/docs/), move it into project root.

2. Install the package (rename the installed package to match the command):
```bash
pip install paperbroker_client-0.2.4-py3-none-any.whl
```

3. Setup environment variables, using [`.env.example`](./.env.example) as reference. The secrets can be found in the private and class channel.

4. Start trading live:
```bash
python paper_trading.py
```