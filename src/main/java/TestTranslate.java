import com.google.cloud.translate.Translate;
import com.google.cloud.translate.TranslateOptions;
import com.google.cloud.translate.Translation;

public class TestTranslate {
    public static void main(String[] args) {
        Translate translate = TranslateOptions.getDefaultInstance().getService();
        System.out.printf("Service opened, app:%s \t project:%s \n", translate.getOptions().getApplicationName(), translate.getOptions().getProjectId());
        // Translate translate = TranslateOptions.getDefaultInstance().getService();

        // Translation translation = translate.translate("¡Hola Mundo!");
		Translation translation = translate.translate("ع 363  بلاد القديم");
        System.out.printf("Translated Text:\n\t%s\n", translation.getTranslatedText());
    }
}
