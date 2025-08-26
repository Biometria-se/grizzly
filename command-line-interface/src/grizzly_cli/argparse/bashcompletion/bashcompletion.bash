_bashcompletion_template() {
    local current previous command

    current="${COMP_WORDS[COMP_CWORD]}"
    previous="${COMP_WORDS[$((COMP_CWORD - 1))]}"

    if (( ${#COMP_WORDS[@]} > 1 )); then
        if [[ "${previous}" == "-"* ]]; then
            command="${COMP_WORDS[*]::${#COMP_WORDS[@]}-2}"
            current="${previous} ${current}"
        else
            command="${COMP_WORDS[*]::${#COMP_WORDS[@]}-1}"
        fi
    else
        command="${COMP_WORDS[*]}"
    fi

    mapfile -t COMPREPLY < <( ${command} --bash-complete="${command} ${current}" )
}

complete -F _bashcompletion_template -o filenames -o noquote bashcompletion_template
