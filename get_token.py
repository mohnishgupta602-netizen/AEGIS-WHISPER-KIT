import os
from dotenv import load_dotenv
from livekit import api

load_dotenv()

api_key = os.getenv("LIVEKIT_API_KEY")
api_secret = os.getenv("LIVEKIT_API_SECRET")

if not api_key or not api_secret or "your_" in api_key:
    print("\n❌ Error: Please put your real LIVEKIT_API_KEY and LIVEKIT_API_SECRET in the .env file first.")
    exit(1)

# Generate a Token for room 'sandbox' (which is the default dev room)
token = api.AccessToken(api_key, api_secret) \
    .with_identity("human_tester") \
    .with_name("Tester") \
    .with_grants(api.VideoGrants(
        room_join=True,
        room="sandbox", 
    )) \
    .to_jwt()

print("\n" + "="*50)
print("COPY THE ENTIRE TEXT BELOW FOR LIVEKIT MEET")
print("="*50)
print(token)
print("="*50 + "\n")
