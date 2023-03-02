
export PATH="/usr/local/bin":$PATH
export SPARK_HOME=/Users/samridhi/spark
export PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
export KAFKA_HOME="/opt/homebrew/Cellar/kafka/3.4.0/libexec" 
export PYSPARK_PYTHON=/usr/bin/python3

# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/Users/samridhi/opt/anaconda3/bin/conda' 'shell.zsh' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/Users/samridhi/opt/anaconda3/etc/profile.d/conda.sh" ]; then
        . "/Users/samridhi/opt/anaconda3/etc/profile.d/conda.sh"
    else
        export PATH="/Users/samridhi/opt/anaconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

