Require cargo

Set Shell "bash"
Set FontSize 16
Set Width 1400
Set Height 700
Set Padding 16
Set Framerate 12
Set PlaybackSpeed 1.25
Set TypingSpeed 25ms
Set Theme "Catppuccin Mocha"
Set WindowBar "Rings"

Hide
Type "export PS1='$ '" Enter
Type "clear" Enter
Show

Type "cargo install --locked --path ." Enter
Sleep 2s

Type "cd ~/src/acme/widgets" Enter
Type "github-reviews register" Enter
Sleep 1s

Type "github-reviews status" Enter
Sleep 2s

Type "github-reviews run --interval 1s --review-command demo-review" Enter
Sleep 7s
Ctrl+C
Sleep 3s
