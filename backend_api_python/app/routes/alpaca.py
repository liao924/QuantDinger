"""
Alpaca Markets API Routes

Standalone API endpoints for US stocks, ETFs, and crypto trading via Alpaca.
Mirrors the structure of routes/ibkr.py for consistency.
"""

from flask import Blueprint, request, jsonify
from app.utils.auth import login_required
from app.utils.logger import get_logger
from app.services.alpaca_trading import AlpacaClient, AlpacaConfig
from app.services.alpaca_trading.client import get_alpaca_client, reset_alpaca_client

logger = get_logger(__name__)

alpaca_bp = Blueprint('alpaca', __name__)

# Global client instance (lazy-init on first request)
_client: AlpacaClient = None


def _get_client() -> AlpacaClient:
    """Get current client instance."""
    global _client
    if _client is None:
        _client = get_alpaca_client()
    return _client


# ==================== Connection Management ====================

@alpaca_bp.route('/status', methods=['GET'])
@login_required
def get_status():
    """Get connection status. GET /api/alpaca/status"""
    try:
        client = _get_client()
        return jsonify({"success": True, "data": client.get_connection_status()})
    except Exception as e:
        logger.error(f"Get status failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@alpaca_bp.route('/connect', methods=['POST'])
@login_required
def connect():
    """
    Connect to Alpaca. POST /api/alpaca/connect
    Body: {
        "apiKey": "PK...",        // Required (PK prefix = paper, AK = live)
        "secretKey": "...",       // Required
        "paper": true,            // Optional, default true
        "baseUrl": ""             // Optional, override
    }
    """
    global _client
    try:
        data = request.get_json() or {}
        api_key = data.get('apiKey', '')
        secret_key = data.get('secretKey', '')
        if not api_key or not secret_key:
            return jsonify({"success": False, "error": "apiKey and secretKey required"}), 400

        config = AlpacaConfig(
            api_key=api_key,
            secret_key=secret_key,
            paper=bool(data.get('paper', True)),
            base_url=data.get('baseUrl') or None,
        )

        # Disconnect existing connection
        if _client is not None and _client.connected:
            _client.disconnect()

        _client = AlpacaClient(config)
        success = _client.connect()
        if success:
            return jsonify({
                "success": True,
                "message": "Connected successfully",
                "data": _client.get_connection_status(),
            })
        return jsonify({
            "success": False,
            "error": "Connection failed. Verify API keys and network access to api.alpaca.markets.",
        }), 400
    except ImportError:
        return jsonify({
            "success": False,
            "error": "alpaca-py not installed. Run: pip install alpaca-py",
        }), 500
    except Exception as e:
        logger.error(f"Alpaca connection failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@alpaca_bp.route('/disconnect', methods=['POST'])
@login_required
def disconnect():
    """Disconnect from Alpaca. POST /api/alpaca/disconnect"""
    global _client
    try:
        if _client is not None:
            _client.disconnect()
            _client = None
        reset_alpaca_client()
        return jsonify({"success": True, "message": "Disconnected"})
    except Exception as e:
        logger.error(f"Disconnect failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== Account Queries ====================

@alpaca_bp.route('/account', methods=['GET'])
@login_required
def get_account():
    """Get account information. GET /api/alpaca/account"""
    try:
        client = _get_client()
        if not client.connected:
            return jsonify({"success": False, "error": "Not connected to Alpaca"}), 400
        return jsonify({"success": True, "data": client.get_account_summary()})
    except Exception as e:
        logger.error(f"Get account info failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@alpaca_bp.route('/positions', methods=['GET'])
@login_required
def get_positions():
    """Get positions. GET /api/alpaca/positions"""
    try:
        client = _get_client()
        if not client.connected:
            return jsonify({"success": False, "error": "Not connected to Alpaca"}), 400
        return jsonify({"success": True, "data": client.get_positions()})
    except Exception as e:
        logger.error(f"Get positions failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@alpaca_bp.route('/orders', methods=['GET'])
@login_required
def get_orders():
    """Get open orders. GET /api/alpaca/orders"""
    try:
        client = _get_client()
        if not client.connected:
            return jsonify({"success": False, "error": "Not connected to Alpaca"}), 400
        return jsonify({"success": True, "data": client.get_open_orders()})
    except Exception as e:
        logger.error(f"Get orders failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== Trading ====================

@alpaca_bp.route('/order', methods=['POST'])
@login_required
def place_order():
    """
    Place an order. POST /api/alpaca/order
    Body: {
        "symbol": "AAPL",           // Required
        "side": "buy",              // Required, buy or sell
        "quantity": 10,             // Required, number of shares
        "marketType": "USStock",    // Optional, USStock or crypto
        "orderType": "market",      // Optional, market or limit
        "price": 150.00,            // Required for limit
        "extendedHours": false      // Optional, for limit orders pre/post-market
    }
    """
    try:
        client = _get_client()
        if not client.connected:
            return jsonify({"success": False, "error": "Not connected to Alpaca"}), 400

        data = request.get_json() or {}
        symbol = data.get('symbol')
        side = data.get('side')
        quantity = data.get('quantity')
        if not symbol:
            return jsonify({"success": False, "error": "Missing symbol"}), 400
        if not side or side.lower() not in ('buy', 'sell'):
            return jsonify({"success": False, "error": "side must be buy or sell"}), 400
        if not quantity or float(quantity) <= 0:
            return jsonify({"success": False, "error": "quantity must be > 0"}), 400

        market_type = data.get('marketType', 'USStock')
        order_type = (data.get('orderType') or 'market').lower()

        if order_type == 'limit':
            price = data.get('price')
            if not price or float(price) <= 0:
                return jsonify({"success": False, "error": "Limit order requires price"}), 400
            result = client.place_limit_order(
                symbol=symbol, side=side, quantity=float(quantity), price=float(price),
                market_type=market_type, extended_hours=bool(data.get('extendedHours', False)),
            )
        else:
            result = client.place_market_order(
                symbol=symbol, side=side, quantity=float(quantity), market_type=market_type,
            )

        if result.success:
            return jsonify({
                "success": True,
                "message": result.message,
                "data": {
                    "orderId": result.order_id, "filled": result.filled,
                    "avgPrice": result.avg_price, "status": result.status, "raw": result.raw,
                },
            })
        return jsonify({"success": False, "error": result.message, "data": result.raw}), 400
    except Exception as e:
        logger.error(f"Place order failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@alpaca_bp.route('/order/<order_id>', methods=['DELETE'])
@login_required
def cancel_order(order_id):
    """Cancel order. DELETE /api/alpaca/order/<order_id>"""
    try:
        client = _get_client()
        if not client.connected:
            return jsonify({"success": False, "error": "Not connected to Alpaca"}), 400
        ok = client.cancel_order(order_id)
        return jsonify({"success": ok, "message": "Cancelled" if ok else "Cancel failed"})
    except Exception as e:
        logger.error(f"Cancel order failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== Market Data ====================

@alpaca_bp.route('/quote/<symbol>', methods=['GET'])
@login_required
def get_quote(symbol):
    """Get real-time quote. GET /api/alpaca/quote/<symbol>?marketType=USStock"""
    try:
        client = _get_client()
        if not client.connected:
            return jsonify({"success": False, "error": "Not connected to Alpaca"}), 400
        market_type = request.args.get('marketType', 'USStock')
        result = client.get_quote(symbol, market_type=market_type)
        if result.get('success'):
            return jsonify({"success": True, "data": result})
        return jsonify({"success": False, "error": result.get('error', 'Quote failed')}), 400
    except Exception as e:
        logger.error(f"Get quote failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
