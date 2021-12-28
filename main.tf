resource "null_resource" "assign" {
  provisioner "local-exec" {
    command = "scripts/crud-assign-lf-tags.ps1 -InputJson $Env:ASSIGN"
    interpreter = ["PowerShell"]
    environment = {
      ASSIGN = jsonencode(tomap(var.assign))
    }
  }
}
