locals {
  prefix = "${var.project_name}-${var.environment}"
}

resource "aws_apigatewayv2_api" "flash_sale" {
  name          = "${local.prefix}-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = var.cors_allowed_origins
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["Content-Type", "Authorization"]
    max_age       = 3600
  }
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.flash_sale.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = var.throttling_burst_limit
    throttling_rate_limit  = var.throttling_rate_limit
  }
}

resource "aws_apigatewayv2_integration" "saga_initiator" {
  api_id                 = aws_apigatewayv2_api.flash_sale.id
  integration_type       = "AWS_PROXY"
  integration_uri        = var.saga_initiator_invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "buy_ticket" {
  api_id    = aws_apigatewayv2_api.flash_sale.id
  route_key = "POST /buy-ticket"
  target    = "integrations/${aws_apigatewayv2_integration.saga_initiator.id}"
}

resource "aws_apigatewayv2_route" "get_status" {
  api_id    = aws_apigatewayv2_api.flash_sale.id
  route_key = "GET /status/{transaction_id}"
  target    = "integrations/${aws_apigatewayv2_integration.saga_initiator.id}"
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.saga_initiator_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.flash_sale.execution_arn}/*/*"
}
