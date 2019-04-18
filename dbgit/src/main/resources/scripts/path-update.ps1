$newPath = $args[0]
$oldPath = [Environment]::GetEnvironmentVariable('path', 'machine');
[Environment]::SetEnvironmentVariable('PATH', "$($newPath);$($oldPath)",'Machine');