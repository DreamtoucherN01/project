package test;

import java.sql.Connection;

import org.junit.Test;

import com.blake.database.DBConnection;
import com.blake.effect.Effect;

public class EffectTest{

	@Test
	public void showEffect() {
		
		DBConnection dbcon = new DBConnection();
		Connection conWorkspace = dbcon.makeWorkspaceConnection();
		
		Effect effect = new Effect(conWorkspace);   
		effect.showMAEAndRMSEEffect();
		effect.showOverlapEffect();        
		effect.showMAPAndNDCGEffect(20);
		effect.showMAPAndNDCGEffect(15);
		effect.showMAPAndNDCGEffect(8);
		effect.showMAPAndNDCGEffect(5);
		effect.showMAPAndNDCGEffect(3);
		effect.showMAPAndNDCGEffect(1);
		
	}
}
