resource "null_resource" "assign" {
  provisioner "local-exec" {
    command = "scripts/crud-assign-lf-tags.ps1 -InputJson $Env:ASSIGN"
    interpreter = ["PowerShell"]
    environment = {
      ASSIGN = jsonencode(tomap(var.assign))
    }
  }
}


resource "null_resource" "grant" {
  provisioner "local-exec" {
    command = format("python3 scripts/grant-revoke-lf-tags.py --data %s", jsonencode(var.grants))
  }
}