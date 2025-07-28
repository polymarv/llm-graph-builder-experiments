import asyncio
import os
import httpx
from neo4j_graphrag.experimental.components.schema import SchemaFromTextExtractor

# Dokumentation: https://neo4j.com/docs/neo4j-graphrag-python/current/api.html#neo4j_graphrag.llm.LLMInterface


# Minimal-LLM -> Fireworks LLM
# Diese Klasse ist eine Wrapper-Klasse für die Fireworks LLM API (kostenlose Version)
class FireworksLLM:
    def __init__(self, model_name: str, model_params: dict = None, api_key: str = None):
        self.base_url = "https://api.fireworks.ai/inference/v1"
        self.model_name = model_name
        self.model_params = model_params or {}
        self.api_key = api_key or os.getenv("FIREWORKS_API_KEY")
        if not self.api_key:
            raise ValueError("FIREWORKS_API_KEY is not set.")

    # Wrapper-Klasse für erwartete Struktur mit `.content`
    class _ResponseWrapper:
        def __init__(self, content: str):
            self.content = content

    async def ainvoke(self, prompt: str, **kwargs):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            **self.model_params,
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/chat/completions", json=payload, headers=headers)

        if response.status_code != 200:
            raise RuntimeError(f"Fireworks API error: {response.status_code} - {response.text}")

        result_text = response.json()["choices"][0]["message"]["content"]
        return self._ResponseWrapper(result_text)

# Async-Hauptfunktion zur Schemaextraktion
async def main():
    extractor = SchemaFromTextExtractor(
        llm=FireworksLLM(
            model_name="accounts/fireworks/models/llama-v3p1-405b-instruct", #accounts/fireworks/models/llama-v3p1-8b-instruct
            model_params={
                "temperature": 0.0,
                "max_tokens": 2000,
                "response_format": {"type": "json_object"},
            },
        )
    )

    text = "Eine Person hat einen Namen und ein Geburtsdatum. Sie arbeitet für eine Firma. Die Firma hat einen Namen und eine Adresse."

    schema = await extractor.run(text=text)

    schema.save("schema_output.json")
    schema.save("schema_output.yaml")

    print("Schema gespeichert")

asyncio.run(main())
