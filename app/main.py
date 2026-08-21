from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.utils import platform

class MTGAPatcherApp(App):
    def build(self):
        self.layout = BoxLayout(orientation='vertical', padding=20, spacing=20)
        
        # 상태 표시 라벨
        self.status_label = Label(
            text="상태: 대기 중\n아래 버튼을 눌러 MTGA Raw 폴더를 선택하세요.",
            halign="center"
        )
        
        # SAF 호출 버튼
        self.btn_select = Button(
            text="MTGA Raw 폴더 선택하기",
            size_hint=(1, 0.2),
            on_press=self.open_saf
        )
        
        self.layout.add_widget(self.status_label)
        self.layout.add_widget(self.btn_select)
        
        return self.layout

    def open_saf(self, instance):
        if platform == 'android':
            from jnius import autoclass
            from android import activity
            
            # 안드로이드 네이티브 Intent 호출 (디렉터리 선택 창 열기)
            Intent = autoclass('android.content.Intent')
            intent = Intent(Intent.ACTION_OPEN_DOCUMENT_TREE)
            
            # 탐색기 결과를 받기 위한 콜백 바인딩 및 실행
            activity.bind(on_activity_result=self.on_activity_result)
            activity.startActivityForResult(intent, 42) # 42는 요청 식별 코드
        else:
            self.status_label.text = "이 기능은 안드로이드 기기에서만 테스트 가능합니다."

    def on_activity_result(self, requestCode, resultCode, intent):
        if requestCode == 42:
            if resultCode == -1: # RESULT_OK (사용자가 폴더를 정상적으로 선택함)
                uri = intent.getData()
                # URI를 텍스트로 변환하여 화면에 출력
                uri_string = uri.toString()
                self.status_label.text = f"권한 획득 성공!\n\n선택된 URI:\n{uri_string}"
                
                # 향후 과제: 이 URI 안에 Raw_CardDatabase_*.mtga 파일이 있는지 검사하는 로직 추가
            else:
                self.status_label.text = "폴더 선택이 취소되었습니다."

if __name__ == '__main__':
    MTGAPatcherApp().run()