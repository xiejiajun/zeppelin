/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {Dataset, DatasetType} from './dataset';

/**
 * Create table data object from paragraph table type result
 */
export default class TableData extends Dataset {
  constructor(columns, rows, comment) {
    super();
    this.columns = columns || [];
    this.rows = rows || [];
    this.comment = comment || '';
  }

  // TODO(Luffy) 加载段落结果
  loadParagraphResult(paragraphResult) {
    if (!paragraphResult || paragraphResult.type !== DatasetType.TABLE) {
      console.log('Can not load paragraph result');
      return;
    }

    let columnNames = [];
    let rows = [];
    let textRows = paragraphResult.msg.split('\n');
    let comment = '';
    let commentRow = false;
    const float64MaxDigits = 16;

    for (let i = 0; i < textRows.length; i++) {
      let textRow = textRows[i];

      if (commentRow) {
        comment += textRow;
        continue;
      }

      if (textRow === '' || textRow === '<!--TABLE_COMMENT-->') {
        if (rows.length > 0) {
          commentRow = true;
        }
        continue;
      }
      let textCols = textRow.split('\t');
      let cols = [];
      for (let j = 0; j < textCols.length; j++) {
        let col = textCols[j];
        if (i === 0) {
          columnNames.push({name: col, index: j, aggr: 'sum'});
        } else {
          let valueOfCol;
          if (!(col[0] === '0' || col[0] === '+' || col.length > float64MaxDigits)) {
            // TODO(Luffy) 这里通过干掉valueOfCol = parseFloat(col)的parseFloat函数调用来解决精度丢失问题
            //  是否太过暴力？
            if (!isNaN(valueOfCol = col) && isFinite(col)) {
              col = valueOfCol;
            }
            // TODO(Luffy) 下面这种方式处理精度丢失问题是否更优雅: parseFloat和parseInt精度丢失原因主要是因为Js整型只支持15位10进制
            //  因为Js整型范围为(-2^53+1, 2^53-1)，2^53为16位10进制，所以2^53-1为15位
            //  参考资料: [【JavaScript】数值精度与数值范围](https://blog.csdn.net/weixin_45389633/article/details/108462073)
            // if (!isNaN(valueOfCol = parseFloat(col)) && isFinite(col) && this.isSafeNumber(valueOfCol)) {
            //   col = valueOfCol;
            // }
          }
          cols.push(col);
        }
      }
      if (i !== 0) {
        rows.push(cols);
      }
    }
    this.comment = comment;
    this.columns = columnNames;
    this.rows = rows;
  }

  isSafeNumber(numberVal) {
    return (numberVal >= Math.pow(-2, 53) + 1) && (numberVal <= Math.pow(2, 53) -1);
  }
}
