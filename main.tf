resource "null_resource" "assign" {
  provisioner "local-exec" {
    command = format("python3 scripts/crud-assign-lf-tags.py --data \"%s\"", jsonencode(var.assign))
  }
}


resource "null_resource" "grant" {
  provisioner "local-exec" {
    command = format("python3 scripts/grant-revoke-lf-tags.py --data \"%s\"", jsonencode(var.grants))
  }
  depends_on = [
    null_resource.assign
  ]
}