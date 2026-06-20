output "table_name" {
  value = aws_dynamodb_table.flash_sale_inventory.name
}

output "table_arn" {
  value = aws_dynamodb_table.flash_sale_inventory.arn
}

output "stream_arn" {
  value = aws_dynamodb_table.flash_sale_inventory.stream_arn
}
