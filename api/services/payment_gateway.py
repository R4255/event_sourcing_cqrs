# First, a mock Razorpay client - replace with real SDK in prod
# api/services/payment_gateway.py

import asyncio


class PaymentGateway:
    async def charge(self, payment_id: str, amount: float, method: str) -> dict:
        """
        In real life: await razorpay_client.payments.fetch(payment_id)
        For now — mock it. If payment_id starts with "fail_" → simulate failure.
        """
        if payment_id.startswith("fail_"):
            return {"success": False, "error": "Payment declined by bank"}
        
        # Simulate network call
        await asyncio.sleep(0.1)
        return {"success": True, "transaction_ref": f"TXN_{payment_id}"}

payment_gateway = PaymentGateway()