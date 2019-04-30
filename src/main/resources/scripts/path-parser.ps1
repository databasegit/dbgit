   $PathArray = @()
   $env:Path.ToString().TrimEnd(';') -split '(?=["])' | ForEach-Object { #remove a trailing semicolon from the path then split it into an array using a double-quote as the delimiter keeping the delimiter
      If ($_ -eq '";') { # throw away a blank line
      } ElseIf ($_.ToString().StartsWith('";')) { # if line starts with "; remove the "; and any trailing backslash
         $PathArray += ($_.ToString().TrimStart('";')).TrimEnd('\')
      } ElseIf ($_.ToString().StartsWith('"')) {  # if line starts with " remove the " and any trailing backslash
         $PathArray += ($_.ToString().TrimStart('"')).TrimEnd('\') #$_ + '"'
      } Else {                                    # split by semicolon and remove any trailing backslash
         $_.ToString().Split(';') | ForEach-Object { If ($_.Length -gt 0) { $PathArray += $_.TrimEnd('\') } }
      }
   }


$dbgitPath = "null"
$PathArray | ForEach-Object { 
   If (Test-Path -Path $_\..\repo\*) {
      If (Get-ChildItem -Path $_\..\dbgit*.jar -Filter dbgit*.jar -Recurse -ErrorAction SilentlyContinue -Force) {$dbgitPath = $_}
   }
}
echo $dbgitPath
