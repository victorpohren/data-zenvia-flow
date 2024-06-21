# Request configuration
from datetime import datetime, timedelta
from zenvia_flow.secrets.get_token import get_secret

ZENVIA_SECRET = ZENVIA_SECRET

last_date = datetime.today()
first_date = last_date - timedelta(days=2)

YESTERDAY = first_date.strftime("%Y-%m-%d")
TODAY = datetime.now().strftime("%Y-%m-%d")
URL = "https://voice-api.zenvia.com/chamada/relatorio"
HEADERS = {
    "Accept": "application/json",
    "Access-Token": ZENVIA_SECRET 
}
PARAMS = {
    "data_inicio": YESTERDAY,
    "data_fim": TODAY,
    "posicao": "0",  # First position
    "limite": "200"
}