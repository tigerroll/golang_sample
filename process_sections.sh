#!/bin/bash

# 入力ファイルのパスを取得
input_file="$1"

# 引数がない場合のUsage表示
if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <input_text_file>"
    exit 1
fi

# 入力ファイルが存在しない場合のチェック
if [ ! -f "$input_file" ]; then
    echo "Error: Input file '$input_file' not found." >&2
    exit 1
fi

# 状態変数
# LookingForSeparator: セパレータ待ち
# LookingForPath: パス行待ち (セパレータ直後)
# InBody: ボディ内容蓄積中 (パス行認識後)
state="LookingForSeparator"

current_output_file="" # 現在処理中の出力ファイルパス
current_body=""        # 現在処理中のボディ内容
line_num=0             # 行番号 (エラー報告用)

# 入力ファイルを1行ずつ読み込む
while IFS= read -r line; do
    line_num=$((line_num + 1))

    # 現在の状態に応じて処理を分岐
    case "$state" in

        "LookingForSeparator")
            # セパレータを探している状態
            if [[ "$line" =~ ^\`\`\` ]]; then
                echo "Separator found at line $line_num."
                # セパレータが見つかったら、次の行はパスのはずなので状態遷移
                state="LookingForPath"
                # 次のセクションのためにパスとボディをリセット (すでに初期状態か前のセクション処理済みのはず)
                current_output_file=""
                current_body=""
            else
                # セパレータが見つかるまでの行は無視（プリアンブルなど）
                : # 何もしない
            fi
            ;;

        "LookingForPath")
            # セパレータの直後で、ファイルパス行を探している状態
            echo "Line $line_num is path candidate: '$line'"

            # パス候補行が空行か '#' で始まるコメント行かをチェック
            if [ -z "$line" ] || [[ "$line" =~ ^# ]]; then
                 echo "Warning at line $line_num: Expected output file path, but found an empty line or comment line ('$line'). Skipping this section." >&2
                 # 無効なパス行なので、このセクション全体をスキップし、次のセパレータを探す状態に戻る
                 state="LookingForSeparator"
                 current_output_file="" #念のためリセット
                 current_body="" #念のためリセット
                 continue # この無効な行の処理をスキップし、次のループへ進む
            fi

            # パス候補行が有効そうであれば、先頭の '// ' を取り除く処理
            processed_path="${line#// }"

            # '// ' を取り除いた結果、パスが空になった場合も無効とする
            if [ -z "$processed_path" ]; then
                echo "Warning at line $line_num: Path line ('$line') resulted in an empty path after removing '// '. Skipping this section." >&2
                # 無効なパス行なので、このセクション全体をスキップし、次のセパレータを探す状態に戻る
                state="LookingForSeparator"
                 current_output_file="" #念のためリセット
                 current_body="" #念のためリセット
                continue # この無効な行の処理をスキップし、次のループへ進む
            else
                 # 有効なファイルパスが見つかったので、ボディ内容蓄積状態に遷移
                 current_output_file="$processed_path"
                 state="InBody"
                 current_body="" # 新しいセクションのボディ蓄積を開始するためリセット
                 echo "Detected valid output file path: '$current_output_file'"
            fi
            ;;

        "InBody")
            # ボディ内容蓄積中の状態
            # 現在の行が次のセパレータかどうかをチェック
            if [[ "$line" =~ ^\`\`\` ]]; then
                echo "Separator found at line $line_num, ending previous section."
                # 次のセパレータが見つかったら、現在のセクションのボディをファイルに書き出す

                if [ -z "$current_output_file" ]; then
                     # InBody状態のはずなのにパスが空なのは異常
                     echo "Error: Entered InBody state at line $line_num without a valid current_output_file set." >&2
                     exit 1 # 致命的なエラーとして終了
                fi

                echo "Writing content to '$current_output_file' ..."
                # 出力先のディレクトリを作成（-p は親ディレクトリも同時に作成し、存在してもエラーにしない）
                output_dir="$(dirname "$current_output_file")"
                if [ ! -d "$output_dir" ]; then
                    mkdir -p "$output_dir"
                    if [ "$?" -ne 0 ]; then
                        echo "Error: Could not create directory '$output_dir'." >&2
                        # ディレクトリ作成失敗時は処理を中断
                        exit 1
                    fi
                fi
                # ボディ内容をファイルに書き出す（上書き）
                # printf "%b\n" を使うことで、内容の最後に改行コードを付加する
                printf "%b\n" "$current_body" > "$current_output_file"
                if [ "$?" -ne 0 ]; then
                     echo "Error: Could not write to file '$current_output_file'." >&2
                     # ファイル書き込み失敗時も処理を中断
                     exit 1
                fi

                # 前のセクションの書き出しが終わったので、新しいセパレータを処理した直後の状態に遷移
                state="LookingForPath" # 今読み込んだ行がセパレータなので、次の行はパスのはず
                current_output_file="" # 新しいセクションのためにパスをリセット
                current_body=""        # 新しいセクションのためにボディをリセット
                # continue は不要。今読んだセパレータ行の処理はこれで終わり。次のループで次の行が読まれる。

            else
                # セパレータではない場合、ボディ内容として蓄積
                # read -r は行末の改行を取り除くため、手動で追加する必要がある
                if [ -z "$current_body" ]; then
                    current_body="$line"
                else
                    current_body="${current_body}\n$line"
                fi
            fi
            ;;

        *)
            # 想定外の状態 (通常は発生しないはず)
            echo "Error: Unknown state: $state" >&2
            exit 1
            ;;
    esac

done < "$input_file"

# ファイルの終端に達した場合の後処理
# 最後に InBody 状態だった場合、最後のセクションのボディを書き出す必要がある
if [ "$state" = "InBody" ]; then
     if [ -z "$current_output_file" ]; then
          # InBody状態のはずなのにパスが空なのは異常
          echo "Error: Input ended in InBody state without a valid current_output_file set." >&2
          exit 1
     fi
     echo "Writing final content to '$current_output_file' ..."
     # 出力先のディレクトリを作成
     output_dir="$(dirname "$current_output_file")"
     if [ ! -d "$output_dir" ]; then
         mkdir -p "$output_dir"
         if [ "$?" -ne 0 ]; then
             echo "Error: Could not create directory '$output_dir'." >&2
             exit 1
         fi
     fi
     # ボディ内容をファイルに書き出す
     # printf "%b\n" を使うことで、内容の最後に改行コードを付加する
     printf "%b\n" "$current_body" > "$current_output_file"
     if [ "$?" -ne 0 ]; then
          echo "Error: Could not write to file '$current_output_file'." >&2
          exit 1
     fi
elif [ "$state" = "LookingForPath" ]; then
     # ファイルがセパレータの直後で終わった場合
     echo "Warning: Input ended immediately after a separator (expected a path line)." >&2
fi
# LookingForSeparator 状態で終わるのは正常な終了（ファイルが空、または最後のセクションが完全に処理された後）

echo "Processing completed."
exit 0
