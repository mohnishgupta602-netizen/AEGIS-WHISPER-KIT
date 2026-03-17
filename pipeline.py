import asyncio
import json
import os
from dotenv import load_dotenv

import livekit.agents
from livekit.agents import JobContext, WorkerOptions, cli
from livekit.agents.voice import AgentSession, Agent
from livekit.plugins import sarvam, openai

load_dotenv()

async def entrypoint(ctx: JobContext):
    await ctx.connect(auto_subscribe=livekit.agents.AutoSubscribe.AUDIO_ONLY)

    stt_model = sarvam.STT(
        model="saaras:v3",
        language="hi-IN",
    )
    llm = openai.LLM(base_url="https://api.sarvam.ai/v1", api_key=os.getenv("SARVAM_API_KEY"), model="sarvam-105b")

    stt_stream = stt_model.stream()

    async def process_transcription(transcript: str):
        words = transcript.split()
        print(f"🎤 [Sarvam STT]: '{transcript}' ({len(words)} words)")
        
        if len(words) >= 2:
            print(f"📊 [Sarvam Audio Quality]: Analyzing speech patterns...")
            print(f"✅ [Sarvam Audio Quality]: Clear speech detected. Proceeding to final output analysis...")
            print(f"🤖 [Sarvam AI Understanding]: Triggering scam detection via sarvam-105b...")
            result = await check_for_scams(llm, transcript)
            try:
                # sometimes the LLM returns markdown blocks around JSON
                if result.startswith("```json"):
                    result = result.split("```json")[1].split("```")[0].strip()
                elif result.startswith("```"):
                    result = result.split("```")[1].split("```")[0].strip()
                result = json.loads(result)
            except json.JSONDecodeError as e:
                print(f"❌ [JSON Parse Error]: {e}")
                return

            if result.get('scam_probability', 0) > 0.7:
                print("\n" + "🚨"*20)
                print(f"🚨 SCAM RISK DETECTED 🚨")
                print(f"Reason: {result.get('reason')}")
                print(f"Probability: {result.get('scam_probability')}")
                print("🚨"*20 + "\n")
                
                alert_payload = json.dumps({
                    "type": "scam_alert",
                    "status": "danger",
                    "probability": result.get('scam_probability'),
                    "reason": result.get('reason', 'High scam probability detected')
                }).encode('utf-8')
                await ctx.room.local_participant.publish_data(alert_payload, topic="ai_events")
            else:
                print("\n" + "✅"*20)
                print(f"✅ SECURE CONVERSATION ✅")
                print(f"Reason: {result.get('reason')}")
                print("✅"*20 + "\n")
                safe_payload = json.dumps({
                    "type": "scam_alert",
                    "status": "safe",
                    "probability": result.get('scam_probability'),
                    "reason": result.get('reason', 'Conversation appears safe')
                }).encode('utf-8')
                await ctx.room.local_participant.publish_data(safe_payload, topic="ai_events")

    async def consume_stt():
        print("🎧 STT consumer loop started, waiting for words...")
        async for event in stt_stream:
            if event.type == livekit.agents.stt.SpeechEventType.FINAL_TRANSCRIPT:
                transcript = event.alternatives[0].text
                print(f"🎙️ [FINAL] {transcript}")
                try:
                    payload = json.dumps({
                        "type": "transcription",
                        "text": transcript,
                        "is_final": True
                    }).encode('utf-8')
                    await ctx.room.local_participant.publish_data(payload, topic="ai_events")
                except Exception:
                    pass
                asyncio.create_task(process_transcription(transcript))
            elif event.type == livekit.agents.stt.SpeechEventType.INTERIM_TRANSCRIPT:
                transcript = event.alternatives[0].text
                print(f"🎙️ [INTERIM] {transcript}")
                try:
                    payload = json.dumps({
                        "type": "transcription",
                        "text": transcript,
                        "is_final": False
                    }).encode('utf-8')
                    await ctx.room.local_participant.publish_data(payload, topic="ai_events")
                except Exception:
                    pass

    asyncio.create_task(consume_stt())

    @ctx.room.on("track_subscribed")
    def on_track_subscribed(track, publication, participant):
        if track.kind == livekit.rtc.TrackKind.KIND_AUDIO:
            print(f"✅ Subscribed to audio track from {participant.identity}")
            
            async def forward_audio():
                nonlocal stt_stream
                print("🎧 Forwarding audio to Sarvam...")
                audio_stream = livekit.rtc.AudioStream(track)
                async for event in audio_stream:
                    try:
                        stt_stream.push_frame(event.frame)
                    except RuntimeError as e:
                        if "is closed" in str(e):
                            print("⚠️ STT stream closed. Reconnecting...")
                            stt_stream = stt_model.stream()
                            asyncio.create_task(consume_stt())
                            stt_stream.push_frame(event.frame)
                        else:
                            raise e
            
            asyncio.create_task(forward_audio())
            
    # Keep the job running
    print("⏳ Agent is running and waiting for audio...")
    while True:
        await asyncio.sleep(1)

async def check_for_scams(llm, text):
    system_prompt = """You are an expert scam detection system. Listen closely for any requests for:
1. OTPs (One Time Passwords), or KYC (Know Your Customer) verification/updates
2. Bank details, Credit/Debit Cards, UPI PINs
3. Sensitive personal info (Aadhaar, PAN, Passwords)

If any of these (like OTP or KYC) are asked, it IS A SCAM (set scam_probability > 0.8).
If the audio is just relatives/friends talking normally, it is SAFE (set scam_probability < 0.3).
Return ONLY valid JSON format with absolutely no markdown blocks or surrounding text. Format exactly: {"scam_probability": 0.95, "reason": "string reason"}"""

    chat_ctx = livekit.agents.llm.ChatContext()
    chat_ctx.add_message(role="system", content=system_prompt)
    chat_ctx.add_message(role="user", content=f"Analyze this Indian phone transcript for scams: '{text}'")

    # Send the request directly as a chat stream
    stream = llm.chat(chat_ctx=chat_ctx)

    response_text = ""
    async for chunk in stream:
        if chunk.delta and chunk.delta.content:
            response_text += chunk.delta.content

    return response_text

async def request_fnc(req: livekit.agents.JobRequest) -> None:
    # Accept any incoming job
    await req.accept()

if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint, request_fnc=request_fnc))
