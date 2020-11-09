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

angular.module('zeppelinWebApp').factory('websocketEvents', WebsocketEventFactory);

function WebsocketEventFactory($rootScope, $websocket, $location, baseUrlSrv, ngToast) {
  'ngInject';

  let websocketCalls = {};
  let pingIntervalId;

  websocketCalls.ws = $websocket(baseUrlSrv.getWebsocketUrl());
  websocketCalls.ws.reconnectIfNotNormalClose = true;

  websocketCalls.ws.onOpen(function() {
    console.log('Websocket created');
    $rootScope.$broadcast('setConnectedStatus', true);
    pingIntervalId = setInterval(function() {
      websocketCalls.sendNewEvent({op: 'PING'});
    }, 10000);
  });

  // TODO js websocket客户端往NotebookServer发送请求的函数，NotebookServer.onMessage接收
  websocketCalls.sendNewEvent = function(data) {
    if ($rootScope.ticket !== undefined) {
      data.principal = $rootScope.ticket.principal;
      data.ticket = $rootScope.ticket.ticket;
      data.roles = $rootScope.ticket.roles;
    } else {
      data.principal = '';
      data.ticket = '';
      data.roles = '';
    }
    console.log('Send >> %o, %o, %o, %o, %o', data.op, data.principal, data.ticket, data.roles, data);
    return websocketCalls.ws.send(JSON.stringify(data));
  };

  websocketCalls.isConnected = function() {
    return (websocketCalls.ws.socket.readyState === 1);
  };

  // TODO js websocket接收NotebookServer.broadcast(String noteId, Message m)推送的消息的处理逻辑
  websocketCalls.ws.onMessage(function(event) {
    let payload;
    if (event.data) {
      payload = angular.fromJson(event.data);
    }

    console.log('Receive << %o, %o', payload.op, payload);

    let op = payload.op;
    let data = payload.data;
    if (op === 'NOTE') {
      $rootScope.$broadcast('setNoteContent', data.note);
    } else if (op === 'NEW_NOTE') {
      $location.path('/notebook/' + data.note.id);
    } else if (op === 'NOTES_INFO') {
      $rootScope.$broadcast('setNoteMenu', data.notes);
    } else if (op === 'LIST_NOTE_JOBS') {
      $rootScope.$emit('jobmanager:set-jobs', data.noteJobs);
    } else if (op === 'LIST_UPDATE_NOTE_JOBS') {
      $rootScope.$emit('jobmanager:update-jobs', data.noteRunningJobs);
    } else if (op === 'AUTH_INFO') {
      let btn = [];
      if ($rootScope.ticket.roles === '[]') {
        btn = [{
          label: 'Close',
          action: function(dialog) {
            dialog.close();
          },
        }];
      } else {
        btn = [{
          label: 'Login',
          action: function(dialog) {
            dialog.close();
            angular.element('#loginModal').modal({
              show: 'true',
            });
          },
        }, {
          label: 'Cancel',
          action: function(dialog) {
            dialog.close();
            // using $rootScope.apply to trigger angular digest cycle
            // changing $location.path inside bootstrap modal wont trigger digest
            $rootScope.$apply(function() {
              $location.path('/');
            });
          },
        }];
      }

      BootstrapDialog.show({
        closable: false,
        closeByBackdrop: false,
        closeByKeyboard: false,
        title: 'Insufficient privileges',
        message: _.escape(data.info.toString()),
        buttons: btn,
      });
    } else if (op === 'PARAGRAPH') {
      // TODO 这个功能主要是为了一人修改, 到处同步，每个打开这个note的人都能看到最新代码，
      //  如果代码只有一个人使用，我们应该可以通过阉割掉这个功能来避免网络抖动导致代码被刷掉的问题
      //  这里最终会调用paragraph.controller.js里面的updateParagraph函数
      //  补充：这个函数还用于段落执行状态更新等，不能直接注释掉来阉割，要阉割的话需要到updateParagraph函数里面
      //      找到替换页面代码(text)的逻辑，再注释掉即可
      $rootScope.$broadcast('updateParagraph', data);
    } else if (op === 'RUN_PARAGRAPH_USING_SPELL') {
      $rootScope.$broadcast('runParagraphUsingSpell', data);
      // TODO 接收执行结果
    } else if (op === 'PARAGRAPH_APPEND_OUTPUT') {
      $rootScope.$broadcast('appendParagraphOutput', data);
    } else if (op === 'PARAGRAPH_UPDATE_OUTPUT') {
      $rootScope.$broadcast('updateParagraphOutput', data);
    } else if (op === 'PROGRESS') {
      $rootScope.$broadcast('updateProgress', data);
    } else if (op === 'COMPLETION_LIST') {
      $rootScope.$broadcast('completionList', data);
    } else if (op === 'EDITOR_SETTING') {
      $rootScope.$broadcast('editorSetting', data);
    } else if (op === 'ANGULAR_OBJECT_UPDATE') {
      $rootScope.$broadcast('angularObjectUpdate', data);
    } else if (op === 'ANGULAR_OBJECT_REMOVE') {
      $rootScope.$broadcast('angularObjectRemove', data);
    } else if (op === 'APP_APPEND_OUTPUT') {
      $rootScope.$broadcast('appendAppOutput', data);
    } else if (op === 'APP_UPDATE_OUTPUT') {
      $rootScope.$broadcast('updateAppOutput', data);
    } else if (op === 'APP_LOAD') {
      $rootScope.$broadcast('appLoad', data);
    } else if (op === 'APP_STATUS_CHANGE') {
      $rootScope.$broadcast('appStatusChange', data);
    } else if (op === 'LIST_REVISION_HISTORY') {
      $rootScope.$broadcast('listRevisionHistory', data);
    } else if (op === 'NOTE_REVISION') {
      $rootScope.$broadcast('noteRevision', data);
    } else if (op === 'NOTE_REVISION_FOR_COMPARE') {
      $rootScope.$broadcast('noteRevisionForCompare', data);
    } else if (op === 'INTERPRETER_BINDINGS') {
      $rootScope.$broadcast('interpreterBindings', data);
    } else if (op === 'SAVE_NOTE_FORMS') {
      $rootScope.$broadcast('saveNoteForms', data);
    } else if (op === 'ERROR_INFO') {
      BootstrapDialog.show({
        closable: false,
        closeByBackdrop: false,
        closeByKeyboard: false,
        title: 'Details',
        message: _.escape(data.info.toString()),
        buttons: [{
          // close all the dialogs when there are error on running all paragraphs
          label: 'Close',
          action: function() {
            BootstrapDialog.closeAll();
          },
        }],
      });
    } else if (op === 'SESSION_LOGOUT') {
      $rootScope.$broadcast('session_logout', data);
    } else if (op === 'CONFIGURATIONS_INFO') {
      $rootScope.$broadcast('configurationsInfo', data);
    } else if (op === 'INTERPRETER_SETTINGS') {
      $rootScope.$broadcast('interpreterSettings', data);
    } else if (op === 'PARAGRAPH_ADDED') {
      $rootScope.$broadcast('addParagraph', data.paragraph, data.index);
    } else if (op === 'PARAGRAPH_REMOVED') {
      $rootScope.$broadcast('removeParagraph', data.id);
    } else if (op === 'PARAGRAPH_MOVED') {
      $rootScope.$broadcast('moveParagraph', data.id, data.index);
    } else if (op === 'NOTE_UPDATED') {
      $rootScope.$broadcast('updateNote', data.name, data.config, data.info);
    } else if (op === 'SET_NOTE_REVISION') {
      $rootScope.$broadcast('setNoteRevisionResult', data);
    } else if (op === 'PARAS_INFO') {
      $rootScope.$broadcast('updateParaInfos', data);
    } else if (op === 'NOTICE') {
      ngToast.info(data.notice);
    } else {
      console.error(`unknown websocket op: ${op}`);
    }
  });

  websocketCalls.ws.onError(function(event) {
    console.log('error message: ', event);
    $rootScope.$broadcast('setConnectedStatus', false);
  });

  websocketCalls.ws.onClose(function(event) {
    console.log('close message: ', event);
    if (pingIntervalId !== undefined) {
      clearInterval(pingIntervalId);
      pingIntervalId = undefined;
    }
    $rootScope.$broadcast('setConnectedStatus', false);
  });

  return websocketCalls;
}
